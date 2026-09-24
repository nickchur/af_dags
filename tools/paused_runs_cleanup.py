"""### ⏸️ DAG: Зависшие раны запаузенных дагов
*2026-09-24 11:26 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Находит раны в `running` / `queued` у дагов на паузе и, если попросили, закрывает их —
как кнопка **Mark failed** в UI.

**Почему они висят.** Шедулер запаузенный даг не разбирает вовсе: не ставит задачи, не
проверяет `dagrun_timeout` (AF 2.11.2, `models/dagrun.py:412`,
`scheduler_job_runner.py:1665`). А `trigger_dag` (кнопка, API, наш сенсор) паузу не смотрит
и создаёт ран сразу в `queued`. Такой ран не начнётся и не кончится, пока паузу не снимут, —
а сняв её, человек получит пачку старых запусков разом. В `get_system_health` это
`runs.paused_active`.

**Что считается зависшим** (порог `older_than_hours`):

| Ран | Возраст считается от | Не трогаем |
|---|---|---|
| `queued` | `queued_at` (у старых ранов без него — `execution_date`) | — |
| `running` | последнего движения: `max(end_date)` задач, иначе `start_date` | ран, где задача в `running`: она доработает (запрос в GP не рвём), и ран уйдёт в следующий проход |

Правило то же, что у санитара `ctl_monitor` (`ctl_worker/ctl_monitor.py`), который на альфе
закрывает такие раны CTL-дагов: отсчёт от движения, а не от старта, иначе ран, шедший пять
часов и запаузенный сейчас, закрылся бы через час.

**Закрытие** (`close=True`) — `set_dag_run_state_to_failed` (`airflow/api/common/mark_tasks.py`),
тот же вызов, что у кнопки: незавершённые задачи → `skipped`, ран → `failed`. Если даг
пропал из сериализации (файл удалён), то же делается запросом к метабазе. На ран пишется
заметка, в журнал `log` — событие `paused_run_failed`. **Паузу даг не снимает** и сами даги
не трогает. Задачи в `scheduled` у таких ранов лежат внутри `running` и уходят в `skipped`;
у рана своего состояния `scheduled` нет.

| Параметр | Описание |
|---|---|
| `older_than_hours` | Порог возраста, ч *(default: `24`)* |
| `states` | Какие раны брать *(default: `running`, `queued`)* |
| `dag_id_like` | Фильтр `dag_id`, SQL `LIKE` (`CTL.%`); пусто — все *(default: пусто)* |
| `close` | Закрывать найденное; иначе только отчёт *(default: `False`)* |
| `max_runs` | Предохранитель: найдено больше — не закрываем ничего, таск красный *(default: `200`)* |
| `schedule` | Расписание: cron или пресет, пусто — только вручную *(default: `0 * * * *`)* |
| `save_params` | Записать форму в Variable `tools_paused_runs_cleanup_params` *(default: `False`)* |

`close` сохраняемый: чтобы закрывали и плановые запуски, один раз запустить с `close` и
`save_params`. До этого плановые запуски только считают.

**Таски:** `params` → `find` → `close`. Отчёт `find` — заметкой на ран: даги, раны, возраст,
задачи по состояниям и **кто поставил паузу** — последнее событие паузы в `log` (UI, API,
CLI). Паузу из кода (`update_dag_pause` CTL, `is_paused_upon_creation`) журнал не пишет —
тогда «нет записи».

Даг создаётся на паузе: закрывающий инструмент не должен включиться сам после выкладки.
"""

from datetime import datetime, timedelta, timezone
from logging import getLogger

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )

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
#: Сколько ранов показываем в заметке; полный список — в логе таска и XCom
NOTE_ROWS = 50
#: Потолок выборки: столько ранов запаузенных дагов не бывает, это защита памяти воркера
FETCH_LIMIT = 5000
LOG_EVENT = 'paused_run_failed'


def _age_h(delta):
    return round(delta.total_seconds() / 3600, 1)


def classify(runs, tis, now, older_than_hours):
    """Решение по ранам без Airflow — чтобы проверялось тестом.

    Args:
        runs: `[{dag_id, run_id, state, queued_at, start_date, execution_date, ...}]`.
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
            since = r.get('queued_at') or r.get('execution_date')
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
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. show_connections.py)
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # Сотня ранов — секунды; потолок про зависшую блокировку в метабазе
        'execution_timeout': timedelta(minutes=10),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    tags=['DataLab', 'tools', 'clean'],
    catchup=False,
    # Закрывающий инструмент не включается сам после выкладки
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=on_callback,
    params={
        'older_than_hours': Param(
            SAVED.get('older_than_hours', 24), type='integer', minimum=1, title='Старше, ч',
            description='queued — от постановки в очередь, running — от последнего движения задач',
        ),
        'states': Param(
            SAVED.get('states', RUN_STATES), type='array', examples=RUN_STATES,
            items={'type': 'string', 'enum': RUN_STATES}, title='Состояния рана',
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
    @task(task_id='find', trigger_rule=TriggerRule.NONE_FAILED)
    def find(**context) -> list:
        """🔎 Раны запаузенных дагов в выбранных состояниях, с возрастом и задачами."""
        from sqlalchemy import func

        from airflow.models import DagModel, DagRun, Log, TaskInstance
        from airflow.utils import timezone as tz
        from airflow.utils.session import create_session

        p = context['params']
        states = [s for s in (p.get('states') or []) if s in RUN_STATES]
        if not states:
            raise ValueError(f"states: нужно хотя бы одно из {RUN_STATES}")
        now = tz.utcnow()

        with create_session() as session:
            q = (session.query(DagRun.dag_id, DagRun.run_id, DagRun.state, DagRun.run_type,
                               DagRun.queued_at, DagRun.start_date, DagRun.execution_date)
                 .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                 .filter(DagModel.is_paused.is_(True), DagRun.state.in_(states)))
            if p.get('dag_id_like'):
                q = q.filter(DagRun.dag_id.like(p['dag_id_like']))
            runs = [r._asdict() for r in q.order_by(DagRun.execution_date).limit(FETCH_LIMIT)]

            tis = {}
            dag_ids = sorted({r['dag_id'] for r in runs})
            if runs:
                keys = {(r['dag_id'], r['run_id']) for r in runs}
                agg = (session.query(TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.state,
                                     func.count(), func.max(TaskInstance.end_date))
                       .filter(TaskInstance.dag_id.in_(dag_ids))
                       .group_by(TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.state))
                for dag_id, run_id, state, n, last_end in agg:
                    if (dag_id, run_id) not in keys:
                        continue
                    info = tis.setdefault((dag_id, run_id), {'states': {}, 'last_end': None})
                    info['states'][state or 'none'] = n
                    if last_end and (info['last_end'] is None or last_end > info['last_end']):
                        info['last_end'] = last_end

            paused_by = {}
            if dag_ids:
                for row in (session.query(Log.dag_id, Log.dttm, Log.event, Log.owner)
                            .filter(Log.dag_id.in_(dag_ids), Log.event.in_(PAUSE_EVENTS))
                            .order_by(Log.dttm.desc())):
                    paused_by.setdefault(row.dag_id, row)

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
        lines = ["| Даг | Ранов | Старейший, ч | Пауза (последняя запись) |", "|---|---:|---:|---|"]
        for dag_id, rs in sorted(by_dag.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"| `{dag_id}` | {len(rs)} | {max(r['age_h'] for r in rs)} | {who(dag_id)} |")
        lines += ["", "| Даг | Ран | Состояние | Возраст, ч | Задачи |", "|---|---|---|---:|---|"]
        for r in sorted(todo, key=lambda r: -r['age_h'])[:NOTE_ROWS]:
            tasks = ', '.join(f"{k} {v}" for k, v in sorted(r['tasks'].items())) or 'нет'
            lines.append(f"| `{r['dag_id']}` | `{r['run_id']}` | {r['state']} | {r['age_h']} | {tasks} |")
        if len(todo) > NOTE_ROWS:
            lines.append(f"| … | ещё {len(todo) - NOTE_ROWS} | | | полный список в логе таска |")
        if busy:
            lines.append(f"\n⏳ Не тронуты — задача ещё в running: {len(busy)} "
                         f"({', '.join(sorted({r['dag_id'] for r in busy})[:10])})")

        title = (f"⏸️ Зависших ранов у запаузенных дагов: {len(todo)} "
                 f"(старше {p['older_than_hours']} ч, дагов {len(by_dag)}; всего активных {len(rows)})")
        logger.info("%s\n%s", title, "\n".join(f"{r['dag_id']} {r['run_id']} {r['state']} {r['age_h']} ч"
                                               for r in todo))
        add_note("\n".join(lines) if todo or busy else "ничего не найдено", context,
                 level='DAG,task', title=title)
        return [{'dag_id': r['dag_id'], 'run_id': r['run_id'], 'state': r['state'],
                 'age_h': r['age_h'], 'paused': who(r['dag_id'])} for r in todo]

    @task(task_id='close', trigger_rule=TriggerRule.NONE_FAILED)
    def close(found: list, **context) -> dict:
        """🧹 Помечает найденные раны failed. Только при close=True и не больше max_runs."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

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

        lines = [f"✅ `{r['dag_id']}` `{r['run_id']}` — {r['how']}" for r in done[:NOTE_ROWS]]
        if len(done) > NOTE_ROWS:
            lines.append(f"… и ещё {len(done) - NOTE_ROWS}")
        lines += [f"❌ `{r['dag_id']}` `{r['run_id']}` — {r['error']}" for r in failed]
        title = f"🧹 Закрыто ранов: {len(done)}, ошибок {len(failed)}"
        add_note("\n".join(lines), context, level='DAG,task', title=title)
        if failed:
            raise AirflowFailException(f"{title}: " + "; ".join(f"{r['dag_id']} {r['run_id']}" for r in failed))
        return {'closed': len(done)}

    found = find()
    save_params() >> found
    close(found)


def _close_run(r, me):
    """Закрывает один ран: как Mark failed, а без сериализованного дага — запросом.

    Returns:
        Как закрыт: `mark failed` или `ORM` (даг не сериализован), со счётом задач.
    """
    import json

    from sqlalchemy import and_, or_

    from airflow.api.common.mark_tasks import set_dag_run_state_to_failed
    from airflow.models import DagRun, Log, TaskInstance
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, State, TaskInstanceState

    with create_session() as session:
        dr = (session.query(DagRun)
              .filter(DagRun.dag_id == r['dag_id'], DagRun.run_id == r['run_id']).one_or_none())
        if dr is None or dr.state not in (DagRunState.RUNNING, DagRunState.QUEUED):
            return f"уже {dr.state if dr else 'удалён'}"

        dag = SerializedDagModel.get_dag(r['dag_id'], session=session)
        how = 'ORM'
        if dag is not None:
            changed = set_dag_run_state_to_failed(dag=dag, run_id=r['run_id'], commit=True, session=session)
            how = f"mark failed, задач {len(changed)}"
        # Добор: задачи, которых нет в текущей версии дага, mark_tasks не видит; без дага —
        # всё здесь. Задачу в running не трогаем: find такие раны не отдаёт
        left = (session.query(TaskInstance)
                .filter(TaskInstance.dag_id == r['dag_id'], TaskInstance.run_id == r['run_id'],
                        or_(TaskInstance.state.is_(None),
                            and_(TaskInstance.state.notin_(list(State.finished)),
                                 TaskInstance.state != TaskInstanceState.RUNNING)))
                .all())
        for ti in left:
            ti.set_state(TaskInstanceState.SKIPPED, session=session)
        if left:
            how += f", добрано skipped {len(left)}"
        # Перечитать, а не refresh: mark_tasks ставит skipped своей сессией (ti.set_state без
        # session) и коммитит сам, наш объект рана после этого отсоединён
        session.flush()
        session.expire_all()
        dr = (session.query(DagRun)
              .filter(DagRun.dag_id == r['dag_id'], DagRun.run_id == r['run_id']).one())
        if dr.state != DagRunState.FAILED:
            dr.set_state(DagRunState.FAILED)

        note = (f"Закрыт {me}: даг на паузе ({r['paused']}), ран висел в {r['state']} "
                f"{r['age_h']} ч")
        dr.note = f"{dr.note}\n\n{note}" if dr.note else note
        session.add(Log(event=LOG_EVENT, owner=me, dag_id=r['dag_id'], run_id=r['run_id'],
                        extra=json.dumps({'state': r['state'], 'age_h': r['age_h'], 'how': how},
                                         ensure_ascii=False)))
    return how


tools_paused_runs_cleanup()
