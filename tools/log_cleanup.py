"""###🛠️ Обслуживание бакета логов
*2026-09-25 19:30 MSK · v2.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Ежедневно создаёт бакет (если не существует), выставляет один срок хранения на весь бакет,
убирает старое и считает статистику по папкам. Бакет берётся из `[logging]
remote_base_log_folder`.

**Один срок на весь бакет** — решение 25.09.2026. Бакет логов — общее хранилище того, что
даги кладут «на память»: под префиксом логов только логи задач, в корне — `ctl/` (снимки
загрузок CTL), `tfs/` (квитанции и очередь ТФС), `docs/` (документация дагов),
`system_health/`, `dag_snapshots/`, дампы `pg_activity/` и `queue_cleanup/`. Своих сроков у
папок нет. Что должно жить дольше срока, его владелец переписывает сам: `test_dags` освежает
копию неизменившегося дага, `tools_mcp_skills` — документы. Неотправленное из `tfs/queue/`
за срок уже не нужно.

Правило жизненного цикла ставится на корень (`DeleteAfter`), правила на папки от прошлых
версий DAG-а (`{префикс}DeleteAfter`) снимаются. На правила не полагаемся: через
корпоративный шлюз они раньше не работали (см. `tools/pg_activity.py`), поэтому уборка всё
равно идёт обходом, а отчёт показывает, сколько объектов старше срока нашлось при обходе.

⚠️ **Правило работает само и асинхронно.** Уменьшили срок — хранилище удалит всё лишнее
своим сканером, без обхода и без отчёта. Проверено на стенде 16.09.2026: прогон с `days=7`
поставил правило, и MinIO сам убрал 23,5 тыс. объектов старше недели. Поэтому разовый
эксперимент с меньшим сроком делайте с `dry_run=True`: он покажет, что будет удалено, и не
станет ни менять правила, ни удалять.

| Параметр | Описание |
|---|---|
| 📅 `days`        | Срок хранения всего бакета (дни, default: `120`; на деве — `30` через `save_params`) |
| ♻️ `lifecycle`   | `True` — выставлять правило жизненного цикла *(default)* |
| 🧪 `dry_run`     | `True` — ничего не удалять и правил не менять, только посчитать |
| ⏱ `max_minutes` | Потолок обхода бакета (минуты, default: `30`) |
| ⏰ `schedule`    | Расписание DAG-а: cron или пресет `@daily`, пусто — только вручную *(default: `17 5 * * *`)* |
| 💾 `save_params` | `True` — сохранить параметры этого запуска как значения по умолчанию, `False` *(default)* |

Значения по умолчанию берутся из переменной `tools_log_cleanup_params`, если она задана,
иначе из кода. Записывается переменная только запуском с `save_params=True` — то есть
разовый эксперимент в UI ночное окно не двигает, а осознанная правка двигает, без выкладки.
Новое расписание подхватывается со следующего парсинга DAG-а; негодное значение таск
`params` не записывает (падает), а уже записанное битым — игнорируется в пользу кода.

**Таски:**
- **params** — сохранение параметров запуска в переменную (пропускается при `save_params=False`)
- **layout** — создание бакета, правило на корень, снятие правил на папки
- **sweep** — один обход бакета: статистика по папкам и удаление всего, что старше срока
- **report** — таблица по папкам в заметку и сводка в XCom
"""

from datetime import datetime, timedelta, timezone
import logging
import time

from airflow.configuration import conf
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.s3_utils import s3_drop_ttl, s3_set_ttl  # type: ignore
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, saved_schedule,
    )
except ImportError:
    from CI06932748.tools.s3_utils import s3_drop_ttl, s3_set_ttl  # type: ignore
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, saved_schedule,
    )

logger = logging.getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Берём из настроек логирования, а не прописываем именем: бакет и соединение
# зависят от контура, а conf читается из файла и переменных окружения — в базу
# на уровне модуля этот вызов не ходит
AWS_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
# s3://dataplatform-monitoring/dataplatform-etl
_LOG_BASE = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1]
BUCKET_NAME = _LOG_BASE.split("/")[0]
# Префикс логов задач: при обходе он идёт последним — он на порядки больше остальных папок
PREFIX = _LOG_BASE[len(BUCKET_NAME):].strip("/")
PREFIX = f"{PREFIX}/" if PREFIX else ""


def _get_paginator(bucket_name=BUCKET_NAME, page_size=1_000, prefix=PREFIX):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
    paginator = s3_hook.get_bucket(bucket_name).meta.client.get_paginator("list_objects_v2")
    return s3_hook, paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, PaginationConfig={'PageSize': page_size}
    )


def _roots(s3_hook, prefix: str = '') -> list:
    """Папки верхнего уровня одним запросом: листинг с разделителем не читает объекты.

    Замер на стенде 16.09.2026: 0.06 с против четырёх минут на обход того же бакета. Нужно,
    чтобы отчёт называл папки, которых нет в карте сроков: без этого они не осматриваются и
    остаются невидимыми до первой аварии.
    """
    client = s3_hook.get_bucket(BUCKET_NAME).meta.client
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/'
    )
    return [item['Prefix'] for page in pages for item in page.get('CommonPrefixes') or []]


def _walk_order(roots: list) -> list:
    """Порядок обхода: папка с логами задач последней.

    Бюджет времени ограничен, а логи задач на порядки больше всего остального и растут с
    каждым прогоном: начав с них, до соседей очередь не дойдёт вовсе — на стенде так пропал
    весь отчёт, кроме одной строки.
    """
    return sorted(roots, key=lambda name: (bool(PREFIX) and PREFIX.startswith(name), name))


# Значения по умолчанию для формы запуска: код задаёт запасной вариант, переменная —
# рабочий. Пишет переменную только запуск с save_params=True, см. таск params.
# Механика повторяет db_cleanup.py; общее на два DAG-а — чтение переменной
# (utils.saved_params) и проверка расписания (utils.valid_schedule).
PARAMS_VAR = 'tools_log_cleanup_params'
DEFAULT_SCHEDULE = '17 5 * * *'
SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


params = {
    'days': _param(
        'days', 120,
        type='integer',
        minimum=1,
        description='Срок хранения всего бакета (дни)',
    ),
    'max_minutes': _param(
        'max_minutes', 30,
        type='integer',
        minimum=1,
        description='Потолок обхода бакета (минуты): не уложились — отчёт скажет, что обход неполный',
    ),
    'dry_run': _param(
        'dry_run', False,
        type='boolean',
        description='True — ничего не удалять и правил не менять: только посчитать и показать',
    ),
    'lifecycle': _param(
        'lifecycle', True,
        type='boolean',
        description='True — выставлять правило жизненного цикла на корень бакета',
    ),
    'schedule': _param(
        'schedule', DEFAULT_SCHEDULE,
        type='string',
        description='Расписание: cron или пресет @daily; пусто — только вручную. Применяется со следующего парсинга',
    ),
    # Разовое действие, а не настройка: в переменную не сохраняется и берётся всегда из кода
    'save_params': Param(
        False,
        type='boolean',
        description='True — сохранить параметры этого запуска как значения по умолчанию',
    ),
}


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 2,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 22, tzinfo=timezone.utc),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    tags=['DataLab', 'tools', 'clean'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    on_failure_callback=on_callback,
    params=params,
)
def tools_log_cleanup():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        status, msg = store_params(PARAMS_VAR, SAVED, context)
        if status == 'skip':
            raise AirflowSkipException(msg)
        if status == 'fail':
            raise AirflowFailException(msg)
        return msg

    # NONE_FAILED, а не дефолтный ALL_SUCCESS: params штатно пропускает себя при
    # save_params=False, а пропуск апстрима по ALL_SUCCESS утягивает в skip всю цепочку
    @task(task_id='layout', trigger_rule=TriggerRule.NONE_FAILED)
    def ensure_layout(**context):
        """🗂 Бакет, правило на весь бакет, снятие правил на папки."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            add_note(f"Создан бакет `{BUCKET_NAME}`", context)

        params = context['params']
        days = int(params['days'])
        if params['dry_run']:
            add_note(f"⚠️ сухой прогон: правила не менялись; срок бакета — {days} дн.", context, title='Сроки хранения')
            return "Сухой прогон: правила не менялись"
        if not params['lifecycle']:
            return "Правила жизненного цикла не выставлялись: `lifecycle=False`"

        note = []
        try:
            s3_set_ttl(AWS_CONN_ID, BUCKET_NAME, days=days, prefix='')
            note.append(f"правило на весь бакет: {days} дн.")
        except Exception as e:
            # Правило — не главный механизм: уборку всё равно делает обход ниже.
            # Через корпоративный шлюз PutBucketLifecycleConfiguration раньше отвечал
            # «Missing required header» (см. pg_activity.py), поэтому падать здесь нельзя
            logger.warning(f"⚠️ правило на бакет не выставлено: {e}")
            note.append(f"⚠️ правило на бакет не выставлено: {e} — уборка обходом")

        # Правила на папки от прошлых версий DAG-а: у объекта срабатывает самый короткий срок,
        # и забытое правило на `system_health/` держало бы его 7 дней вместо общего
        try:
            rules = s3_hook.get_conn().get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME).get('Rules', [])
        except Exception as e:
            logger.warning(f"⚠️ правила бакета не прочитаны: {e}")
            rules = []
        dropped = []
        for rule in rules:
            prefix = (rule.get('Filter') or {}).get('Prefix', '')
            if prefix and rule.get('ID') == f'{prefix}DeleteAfter':
                try:
                    if s3_drop_ttl(AWS_CONN_ID, BUCKET_NAME, prefix=prefix):
                        dropped.append(prefix)
                except Exception as e:
                    logger.warning(f"⚠️ правило для `{prefix}` не снято: {e}")
                    note.append(f"⚠️ правило `{prefix}` не снято: {e}")
        if dropped:
            note.append(f"сняты правила на папки: {', '.join(f'`{p}`' for p in dropped)}")
        add_note("\n".join(note), context, title='Сроки хранения')
        return "; ".join(note)

    @task
    def sweep(**context):
        """🧹 Обход бакета: статистика по папкам верхнего уровня и удаление старше срока.

        Обход один на всё: листинг и удаление за проход, а не два прохода подряд — на бакете
        с сотнями тысяч объектов второй листинг стоит столько же, сколько первый (замер на
        стенде 16.09.2026: MinIO отдавал около 100 объектов в секунду, 24 тысячи — четыре
        минуты). Отсюда же потолок по времени: не уложились — отчёт скажет, что данные
        неполные, а слот пула не держим до бесконечности. Объекты прямо в корне бакета (без
        папки) не обходятся: наши даги так не пишут.
        """
        params = context['params']
        days = int(params['days'])
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)
        deadline = time.monotonic() + params['max_minutes'] * 60

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        stats: dict[str, dict] = {}
        batch: list[str] = []
        partial = False

        # По папкам верхнего уровня, логи задач последними: иначе до остальных не дойдёт бюджет
        for walk in _walk_order(_roots(s3_hook)):
            _, pages = _get_paginator(prefix=walk)
            for page in pages:
                for obj in page.get("Contents") or []:
                    key, size, modified = obj["Key"], obj.get("Size", 0), obj["LastModified"]
                    row = stats.setdefault(
                        walk,
                        {'objects': 0, 'bytes': 0, 'oldest': None, 'newest': None,
                         'types': {}, 'days': days, 'over_ttl': 0, 'deleted': 0, 'deleted_bytes': 0},
                    )
                    row['objects'] += 1
                    row['bytes'] += size
                    row['oldest'] = min(row['oldest'] or modified, modified)
                    row['newest'] = max(row['newest'] or modified, modified)
                    suffix = key.rsplit('.', 1)[-1][:8] if '.' in key.rsplit('/', 1)[-1] else 'без типа'
                    row['types'][suffix] = row['types'].get(suffix, 0) + 1

                    if modified > cutoff:
                        continue
                    row['over_ttl'] += 1
                    if params['dry_run']:
                        continue
                    row['deleted'] += 1
                    row['deleted_bytes'] += size
                    batch.append(key)
                    if len(batch) >= 1_000:
                        s3_hook.delete_objects(bucket=BUCKET_NAME, keys=batch)
                        batch = []
                if time.monotonic() > deadline:
                    partial = True
                    logger.warning(f"⚠️ обход прерван по времени на `{walk}`")
                    break
            if partial:
                break
        if batch:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=batch)

        for row in stats.values():
            row['oldest'] = row['oldest'].isoformat(timespec='seconds') if row['oldest'] else None
            row['newest'] = row['newest'].isoformat(timespec='seconds') if row['newest'] else None
        deleted = sum(row['deleted'] for row in stats.values())
        over = sum(row['over_ttl'] for row in stats.values())
        head = f"Сухой прогон: удалить нужно {over}" if params['dry_run'] else f"Удалено {deleted} объектов"
        add_note(f"{head}, папок осмотрено {len(stats)}", context)
        return {
            'bucket': BUCKET_NAME,
            'days': days,
            'dry_run': bool(params['dry_run']),
            'checked_at': now.isoformat(timespec='seconds'),
            'partial': partial,
            'folders': stats,
        }

    @task
    def report(swept: dict, **context):
        """📋 Таблица по папкам в заметку, сводка — в XCom.

        Важное первым: заметка режется по MAX_NOTE_LEN, и в хвост должны уходить самые
        мелкие папки, а не итог.
        """
        folders = swept['folders']
        total_objects = sum(row['objects'] for row in folders.values())
        total_bytes = sum(row['bytes'] for row in folders.values())
        deleted = sum(row['deleted'] for row in folders.values())
        over_ttl = {name: row['over_ttl'] for name, row in folders.items() if row['over_ttl']}

        lines = [
            f"`{swept['bucket']}`: {readable_size(total_bytes)}, объектов {readable_size(total_objects, 1000)}, "
            f"срок {swept.get('days')} дн., удалено {deleted}" + (" (сухой прогон)" if swept.get('dry_run') else ""),
            "",
            "| Папка | Объектов | Объём | Старейший | Удалено |",
            "|---|---|---|---|---|",
        ]
        for name, row in sorted(folders.items(), key=lambda item: -item[1]['objects']):
            oldest = (row['oldest'] or '')[:10]
            lines.append(
                f"| `{name}` | {row['objects']} | {readable_size(row['bytes'])} | {oldest} | {row['deleted']} |"
            )
        if swept.get('partial'):
            lines.insert(1, "⚠️ обход не закончен: данные неполные, поднимите `max_minutes` или разберитесь, "
                            "почему листинг медленный")
        if over_ttl:
            # Сколько объектов старше срока дожило до обхода. Правило жизненного цикла
            # применяется хранилищем асинхронно, поэтому сразу после смены срока это число
            # большое и это нормально; если оно не падает от прогона к прогону — правила на
            # шлюзе не работают, и бакет держит обход
            lines.insert(1, f"объектов старше срока при обходе: {over_ttl}")
        add_note("\n".join(lines), context, title='Бакет логов', level='task,DAG')
        return {**swept, 'totals': {'objects': total_objects, 'bytes': total_bytes, 'deleted': deleted}}

    report(save_params() >> ensure_layout() >> sweep())


tools_log_cleanup()
