"""### ⏸️ DAG: Зависшие раны запаузенных дагов
*2026-10-02 07:26 MSK · v1.9 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в час находит раны в `running` / `queued` у дагов на паузе старше `older_than_hours` и,
если попросили, закрывает их как **Mark failed**: незавершённые задачи → `skipped`, ран →
`failed`. Шедулер запаузенный даг не разбирает, а `trigger_dag` паузу не смотрит — такой ран
иначе висит вечно. Паузу даг не снимает. Создаётся на паузе.

**Таски:** `params` → `collect` (отчёт: раны, задачи, кто поставил паузу) → `close`.

Подробно: [tools/readme.md — paused_runs_cleanup](../../_plugin_dag_docs/?doc=tools/readme.md#paused_runs_cleanuppypaused_runs_cleanuppy)
"""

from datetime import datetime, timedelta, timezone
from logging import getLogger
from types import SimpleNamespace

from airflow.sdk import Param, TriggerRule, dag, task

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, Feed, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, Feed, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )

try:
    from plugins import af_meta  # type: ignore
except ImportError:
    from CI06932748.tools import af_meta  # type: ignore

logger = getLogger("airflow.task")

MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

PARAMS_VAR = 'tools_paused_runs_cleanup_params'
SAVED = saved_params(PARAMS_VAR)
# Раз в час: порог в сутки, точнее не нужно, а запрос дешёвый (индекс по состоянию рана)
DEFAULT_SCHEDULE = '0 * * * *'
RUN_STATES = ['running', 'queued']

#: События журнала, которыми Airflow пишет паузу: UI (`paused`, с 2.10 и `ui.paused`),
#: REST API (`patch_dag`), CLI. Пауза из кода записи не оставляет.
PAUSE_EVENTS = ('paused', 'ui.paused', 'patch_dag', 'api.patch_dag', 'cli_dag_pause')
#: Потолок выборки: столько ранов запаузенных дагов не бывает, это защита памяти воркера
FETCH_LIMIT = 5000
LOG_EVENT = 'paused_run_failed'


def _age_h(delta):
    return round(delta.total_seconds() / 3600, 1)


def classify(runs, tis, now, older_than_hours):
    """Решение по ранам без Airflow — чтобы проверялось тестом.

    Args:
        runs: `[{dag_id, run_id, state, queued_at, start_date, run_after, ...}]`.
        tis: `{(dag_id, run_id): {'states': {state: n}, 'last_end': datetime|None}}`.
        now: Текущее время (aware).
        older_than_hours: Порог.

    Returns:
        Список записей с `age_h`, `tasks`, `verdict`: `close` — зависший,
        `busy` — задача ещё в running, `young` — моложе порога.
    """
    limit = timedelta(hours=older_than_hours)
    out = []
    for r in runs:
        info = tis.get((r['dag_id'], r['run_id'])) or {}
        states = info.get('states') or {}
        if r['state'] == 'queued':
            since = r.get('queued_at') or r.get('run_after')
        else:
            since = info.get('last_end') or r.get('start_date') or r.get('queued_at')
        age = now - since if since else timedelta(0)
        if r['state'] == 'running' and states.get('running'):
            verdict = 'busy'
        elif age >= limit:
            verdict = 'close'
        else:
            verdict = 'young'
        out.append({**r, 'since': since, 'age_h': _age_h(age), 'tasks': states, 'verdict': verdict})
    return out


@dag(
    doc_md=__doc__,
    description='Зависшие раны запаузенных дагов: отчёт и, по галке, Mark failed',
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. test_connections.py)
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # Сотня ранов — секунды; потолок про зависшую блокировку в метабазе
        'execution_timeout': timedelta(minutes=10),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    tags=['DataTools', 'tools', 'clean'],
    catchup=False,
    # Закрывающий инструмент не включается сам после выкладки
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=on_callback,
    params={
        'older_than_hours': Param(
            SAVED.get('older_than_hours', 24), type='integer', minimum=1, title='Старше, ч',
            description='Порог возраста рана, ч: queued — от постановки в очередь, running — от последнего движения задач',
        ),
        'states': Param(
            SAVED.get('states', RUN_STATES), type='array', examples=RUN_STATES,
            items={'type': 'string', 'enum': RUN_STATES}, title='Состояния рана',
            description='Какие раны брать: queued — возраст от постановки в очередь, running — от последнего движения задач',
        ),
        'dag_id_like': Param(
            SAVED.get('dag_id_like', ''), type=['string', 'null'], title='Фильтр dag_id',
            description='SQL LIKE, например CTL.%; пусто — все даги',
        ),
        'close': Param(
            bool(SAVED.get('close', False)), type='boolean', title='Закрывать',
            description='Пометить найденные раны failed (как Mark failed). Без галочки — только отчёт',
        ),
        'max_runs': Param(
            SAVED.get('max_runs', 200), type='integer', minimum=0, title='Предохранитель',
            description='Найдено больше — не закрываем ничего, таск красный',
        ),
        'schedule': Param(
            SAVED.get('schedule', DEFAULT_SCHEDULE), type=['string', 'null'], title='Расписание',
            description='cron или пресет (@daily); пусто — только вручную. Применяется со следующего разбора',
        ),
        'save_params': Param(
            False, type='boolean', title='Сохранить параметры',
            description=f'Записать параметры этого запуска в {PARAMS_VAR}: по ним пойдут и плановые запуски',
        ),
    },
)
def tools_paused_runs_cleanup():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет форму (в т. ч. close и расписание) как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context)

    # NONE_FAILED: params штатно пропускает себя без save_params. Если же он упал (битое
    # расписание), дальше не идём: форма негодная — закрывать по ней нельзя
    @task(task_id='collect', trigger_rule=TriggerRule.NONE_FAILED)
    def collect(**context) -> list:
        """🔎 Раны запаузенных дагов в выбранных состояниях, с возрастом и задачами."""
        p = context['params']
        states = [s for s in (p.get('states') or []) if s in RUN_STATES]
        if not states:
            raise ValueError(f"states: нужно хотя бы одно из {RUN_STATES}")
        now = datetime.now(timezone.utc)

        # Airflow 3: ORM у задачи нет — те же выборки SQL-ом через af_meta
        runs = af_meta.sql(
            "SELECT dr.dag_id, dr.run_id, dr.state, dr.run_type, dr.queued_at, dr.start_date, dr.run_after"
            "  FROM dag_run dr JOIN dag d ON d.dag_id = dr.dag_id"
            " WHERE d.is_paused AND dr.state = ANY(%(states)s)"
            + (" AND dr.dag_id LIKE %(like)s" if p.get('dag_id_like') else "")
            + " ORDER BY dr.run_after LIMIT %(lim)s",
            {'states': states, 'like': p.get('dag_id_like'), 'lim': FETCH_LIMIT})

        tis = {}
        dag_ids = sorted({r['dag_id'] for r in runs})
        if runs:
            # По парам (dag_id, run_id) — индекс ti_dag_run; по одному dag_id читалась
            # вся история запаузенных дагов, а лишнее отбрасывалось уже здесь
            agg = af_meta.sql(
                "SELECT ti.dag_id, ti.run_id, ti.state, count(*) AS n, max(ti.end_date) AS last_end"
                "  FROM task_instance ti"
                "  JOIN unnest(%(dags)s::text[], %(runs)s::text[]) AS k(dag_id, run_id)"
                "    ON ti.dag_id = k.dag_id AND ti.run_id = k.run_id"
                " GROUP BY ti.dag_id, ti.run_id, ti.state",
                {'dags': [r['dag_id'] for r in runs], 'runs': [r['run_id'] for r in runs]})
            for row in agg:
                info = tis.setdefault((row['dag_id'], row['run_id']), {'states': {}, 'last_end': None})
                info['states'][row['state'] or 'none'] = row['n']
                last_end = row['last_end']
                if last_end and (info['last_end'] is None or last_end > info['last_end']):
                    info['last_end'] = last_end

        paused_by = {}
        if dag_ids:
            for row in af_meta.sql(
                    "SELECT dag_id, dttm, event, owner FROM log"
                    " WHERE dag_id = ANY(%(dags)s) AND event = ANY(%(events)s) ORDER BY dttm DESC",
                    {'dags': dag_ids, 'events': list(PAUSE_EVENTS)}):
                paused_by.setdefault(row['dag_id'], SimpleNamespace(**row))

        rows = classify(runs, tis, now, p['older_than_hours'])
        todo = [r for r in rows if r['verdict'] == 'close']
        busy = [r for r in rows if r['verdict'] == 'busy']

        def who(dag_id):
            ev = paused_by.get(dag_id)
            if not ev:
                return 'нет записи'
            return f"{ev.event} · {ev.owner or '?'} · {ev.dttm.astimezone(MSK):%Y-%m-%d %H:%M}"

        by_dag = {}
        for r in todo:
            by_dag.setdefault(r['dag_id'], []).append(r)
        # Строка на даг, затем раны от старых; таблица вытесняла строки из 1000 символов заметки
        lines = [f"⏸️ `{dag_id}`: ранов {len(rs)}, старейший {max(r['age_h'] for r in rs)} ч · пауза: {who(dag_id)}"
                 for dag_id, rs in sorted(by_dag.items(), key=lambda kv: -len(kv[1]))]
        for r in sorted(todo, key=lambda r: -r['age_h']):
            tasks = ', '.join(f"{k} {v}" for k, v in sorted(r['tasks'].items())) or 'задач нет'
            lines.append(f"`{r['dag_id']}` `{r['run_id']}`: {r['state']}, {r['age_h']} ч · {tasks}")
        rest = (f"⏳ не тронуты — задача ещё в running: {len(busy)} "
                f"({', '.join(sorted({r['dag_id'] for r in busy})[:10])})" if busy else '')

        summary = (f"зависших ранов {len(todo)} у {len(by_dag)} дагов (старше {p['older_than_hours']} ч; "
                   f"всего активных {len(rows)})" if todo or busy else "ничего не найдено")
        logger.info("%s\n%s", summary, "\n".join(f"{r['dag_id']} {r['run_id']} {r['state']} {r['age_h']} ч"
                                                 for r in todo))
        Feed(context, '⏸️ collect').done(summary, keep=lines, rest=rest, dag=True)
        return [{'dag_id': r['dag_id'], 'run_id': r['run_id'], 'state': r['state'],
                 'age_h': r['age_h'], 'paused': who(r['dag_id'])} for r in todo]

    @task(task_id='close', trigger_rule=TriggerRule.NONE_FAILED)
    def close(found: list, **context) -> dict:
        """🧹 Помечает найденные раны failed. Только при close=True и не больше max_runs."""
        from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

        p = context['params']
        if not p.get('close'):
            raise AirflowSkipException(f"close=False — только отчёт ({len(found)} ранов)")
        if not found:
            raise AirflowSkipException("закрывать нечего")
        if len(found) > p['max_runs']:
            raise AirflowFailException(
                f"найдено {len(found)} ранов > max_runs={p['max_runs']}: не закрыто ничего. "
                "Проверьте список в заметке и поднимите max_runs для разового запуска")

        me = context['dag'].dag_id
        done, failed = [], []
        for r in found:
            try:
                done.append({**r, 'how': _close_run(r, me)})
            except Exception as e:  # один ран не должен останавливать остальные
                logger.exception("❌ %s %s", r['dag_id'], r['run_id'])
                failed.append({**r, 'error': f"{type(e).__name__}: {e}"})

        lines = ([f"❌ `{r['dag_id']}` `{r['run_id']}` — {r['error']}" for r in failed]
                 + [f"✅ `{r['dag_id']}` `{r['run_id']}` — {r['how']}" for r in done])
        title = f"закрыто ранов {len(done)}, ошибок {len(failed)}"
        Feed(context, '🧹 close').done(title, keep=lines, dag=True)
        if failed:
            raise AirflowFailException(f"{title}: " + "; ".join(f"{r['dag_id']} {r['run_id']}" for r in failed))
        return {'closed': len(done)}

    found = collect()
    save_params() >> found
    close(found)


def _close_run(r, me):
    """Закрывает один ран одной сессией: задачи → skipped одним UPDATE, ран → failed.

    Не через `set_dag_run_state_to_failed` — почему, см. шапку модуля.

    Returns:
        Сколько задач ушло в skipped, или «уже <состояние>».
    """
    import json

    from sqlalchemy import text

    # Airflow 3: ORM у задачи нет — то же одной транзакцией через af_meta. set_state рана
    # (state = failed и end_date) повторён UPDATE-ом; заметка — в dag_run_note
    with af_meta.meta_session() as session:
        row = session.execute(text(
            "SELECT id, state FROM dag_run WHERE dag_id = :d AND run_id = :r FOR UPDATE"),
            {'d': r['dag_id'], 'r': r['run_id']}).mappings().first()
        if row is None or row['state'] not in ('running', 'queued'):
            return f"уже {row['state'] if row else 'удалён'}"

        mine = {'d': r['dag_id'], 'r': r['run_id']}
        # Задача в running: collect такие раны не отдаёт, но она могла стартовать между collect и
        # close. Не трогаем — доработает, и ран закроется в следующий проход
        if session.execute(text(
                "SELECT 1 FROM task_instance WHERE dag_id = :d AND run_id = :r AND state = 'running' LIMIT 1"),
                mine).first():
            return "оставлен: задача в running"
        n = session.execute(text(
            "UPDATE task_instance SET state = 'skipped', end_date = now(), updated_at = now()"
            " WHERE dag_id = :d AND run_id = :r"
            "   AND (state IS NULL OR state NOT IN ('success', 'failed', 'skipped', 'upstream_failed', 'removed'))"),
            mine).rowcount
        how = f"skipped задач {n}"
        session.execute(text("UPDATE dag_run SET state = 'failed', end_date = now(), updated_at = now() WHERE id = :id"),
                        {'id': row['id']})

        note = (f"Закрыт {me}: даг на паузе ({r['paused']}), ран висел в {r['state']} "
                f"{r['age_h']} ч")
        session.execute(text(
            "INSERT INTO dag_run_note (dag_run_id, content, created_at, updated_at)"
            " VALUES (:id, :note, now(), now())"
            " ON CONFLICT (dag_run_id) DO UPDATE"
            "   SET content = coalesce(dag_run_note.content || E'\\n\\n', '') || :note, updated_at = now()"),
            {'id': row['id'], 'note': note})
        session.execute(text(
            "INSERT INTO log (dttm, event, owner, dag_id, run_id, extra) VALUES (now(), :e, :o, :d, :r, :x)"),
            {'e': LOG_EVENT, 'o': me, **mine,
             'x': json.dumps({'state': r['state'], 'age_h': r['age_h'], 'how': how}, ensure_ascii=False)})
    return how


tools_paused_runs_cleanup()
