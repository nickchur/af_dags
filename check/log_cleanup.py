"""###🛠️ Обслуживание бакета логов задач
*2026-09-16 11:46 MSK · v1.7 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Ежедневно создаёт бакет (если не существует), выставляет сроки хранения по папкам, убирает
старое и считает статистику. Бакет берётся из `[logging] remote_base_log_folder`, то есть
обслуживается бакет логов задач.

Папок в S3 нет — есть префиксы, поэтому «заводится» не каталог, а срок: на каждый префикс
ставится правило жизненного цикла (`{префикс}DeleteAfter`). На правила не полагаемся: через
корпоративный шлюз они раньше не работали (см. `check/pg_activity.py`), поэтому уборка всё
равно идёт обходом, а отчёт показывает, сколько объектов старше срока нашлось при обходе.

⚠️ **Правило работает само и асинхронно.** Уменьшили срок — хранилище удалит всё лишнее
своим сканером, без обхода и без отчёта. Проверено на стенде 16.09.2026: прогон с `days=7`
поставил правило, и MinIO сам убрал 23,5 тыс. объектов старше недели. Поэтому разовый
эксперимент с меньшим сроком делайте с `dry_run=True`: он покажет, что будет удалено, и не
станет ни менять правила, ни удалять.

| Параметр | Описание |
|---|---|
| 📅 `days`        | Срок хранения логов задач (дни, default: `30`) |
| 🗂 `folders`     | Сроки остальных папок бакета: префикс → дни |
| 🧹 `other_days`  | Срок для папок, которых нет в списке; `0` — не трогать *(default)* |
| ♻️ `lifecycle`   | `True` — выставлять правила жизненного цикла *(default)* |
| 🧪 `dry_run`     | `True` — ничего не удалять и правил не менять, только посчитать |
| ⏱ `max_minutes` | Потолок обхода бакета (минуты, default: `30`) |
| ⏰ `schedule`    | Расписание DAG-а: cron или пресет `@daily`, пусто — только вручную *(default: `17 5 * * *`)* |
| 💾 `save_params` | `True` — сохранить параметры этого запуска как значения по умолчанию, `False` *(default)* |

Значения по умолчанию берутся из переменной `tools_log_cleanup_params`, если она задана,
иначе из кода. Записывается переменная только запуском с `save_params=True` — то есть
разовый эксперимент в UI ночное окно не двигает, а осознанная правка двигает, без выкладки.
Новое расписание подхватывается со следующего парсинга DAG-а; негодное значение таск
`params` не записывает (падает), а уже записанное битым — игнорируется в пользу кода.

Соседние даги свои дампы убирают сами и знают про свои ключи больше: `pg_activity` и
`queue_cleanup` удаляют по дате В ИМЕНИ ключа и не трогают то, что на их формат не похоже, а
`test_dags` держит версии, пока жив сам даг. Поэтому этот DAG им страховка, а не замена:
сроки здесь стоят не меньше их собственных `keep_days`, а снимкам дагов срок не задан вовсе —
их возраст ничего не значит. Действующий срок папки — меньший из двух.

**Таски:**
- **params** — сохранение параметров запуска в переменную (пропускается при `save_params=False`)
- **layout** — создание бакета и сроков хранения по папкам
- **sweep** — один обход бакета: статистика по папкам и удаление всего, что старше своего срока
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
    from CI06932748.tools.s3_utils import s3_set_ttl  # type: ignore
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, valid_schedule,
    )
except ImportError:
    from plugins.s3_utils import s3_set_ttl  # type: ignore
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, valid_schedule,
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
# Всё, что после имени бакета. Без префикса удаление шло бы по всему бакету, а он
# общий: кроме логов задач там лежит чужое, и чистить его этот DAG не должен
PREFIX = _LOG_BASE[len(BUCKET_NAME):].strip("/")
PREFIX = f"{PREFIX}/" if PREFIX else ""


def _get_paginator(bucket_name=BUCKET_NAME, page_size=1_000, prefix=PREFIX):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
    paginator = s3_hook.get_bucket(bucket_name).meta.client.get_paginator("list_objects_v2")
    return s3_hook, paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, PaginationConfig={'PageSize': page_size}
    )


def _folder_days(params) -> dict:
    """Сроки по папкам: логи задач из `days`, остальные из `folders`.

    Ключ — префикс от корня бакета, всегда со слешем на конце: по нему и сверяются ключи
    объектов при обходе.
    """
    folders = {PREFIX: int(params['days'])} if PREFIX else {}
    for prefix, days in (params.get('folders') or {}).items():
        name = str(prefix).strip().strip('/')
        if name:
            folders[f"{name}/"] = int(days)
    return folders


def _default_days(params) -> int:
    """Срок для объектов вне известных папок.

    Если логи задач лежат в корне бакета (префикса нет), то «прочее» — это и есть они, и
    срок берётся их собственный, а не `other_days`.
    """
    return int(params['days'] if not PREFIX else params['other_days'])


def _roots(s3_hook, prefix: str = '') -> list:
    """Папки верхнего уровня одним запросом: листинг с разделителем не читает объекты.

    Замер на стенде 16.09.2026: 0.06 с против четырёх минут на обход того же бакета. Нужно,
    чтобы отчёт называл папки, которых нет в карте сроков: без этого они не осматриваются и
    остаются невидимыми до первой аварии.
    """
    client = s3_hook.get_bucket(BUCKET_NAME).meta.client
    answer = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    return [item['Prefix'] for item in answer.get('CommonPrefixes') or []]


def _prefix_of(key: str, folders: dict):
    """Папка объекта: самый длинный из известных префиксов, иначе None («прочее»)."""
    matches = [prefix for prefix in folders if key.startswith(prefix)]
    return max(matches, key=len) if matches else None


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


def _schedule():
    """Расписание DAG-а: из переменной, если оно осмысленное, иначе из кода."""
    value = SAVED.get('schedule', DEFAULT_SCHEDULE)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {PARAMS_VAR}: расписание '{value}' не разобрано — беру {DEFAULT_SCHEDULE}")
        return DEFAULT_SCHEDULE
    return None if value in (None, '', 'None') else str(value).strip()


# Папки бакета, которые заводит не этот DAG: логи ядра пишет платформа (`_health/` — отбивки
# воркеров), дампы кладут соседние даги. Сроки соседей не меньше их собственных keep_days:
# страховка не должна удалять раньше хозяина
# Отбивки воркеров и проба здоровья пишутся ПОД префиксом логов (их кладёт ядро и
# check/system_health.py по remote_base_log_folder), а дампы соседних дагов — в корне бакета,
# чтобы под чистку логов не попадать. Ключи здесь — от корня бакета, как их видит листинг
DEFAULT_FOLDERS = {
    f'{PREFIX}_health/': 7,
    f'{PREFIX}_system_health/': 7,
    # Снимки дагов по возрасту чистить НЕЛЬЗЯ: test_dags держит там все версии живых дагов и
    # удаляет только снимки дагов, которых больше нет. Даты в ключе нет, и у дага, не
    # менявшегося полгода, единственная версия — полугодовой давности: удалив её, мы лишим
    # сравнение базы. Ноль — «не трогать»
    'dag_snapshots/': 0,
    # Тракт ТФС живёт в корне этого же бакета (`plugins/tfs_utils.py`, S3_UNDER_LOG_PREFIX =
    # False). Возраст там ничего не значит: в `tfs/queue/` лежат файлы, ждущие отправки, и в
    # паузе отправки они ждут сколько угодно. Ноль — «не трогать»
    'tfs/': 0,
    'queue_cleanup/': 30,
    'pg_activity/': 30,
}

params = {
    'days': _param(
        'days', 30,
        type='integer',
        minimum=1,
        description='Срок хранения логов задач (дни)',
    ),
    'folders': _param(
        'folders', DEFAULT_FOLDERS,
        type='object',
        description='Сроки остальных папок бакета: префикс → дни',
    ),
    'other_days': _param(
        'other_days', 0,
        type='integer',
        minimum=0,
        description='Срок для папок, которых нет в списке; 0 — не трогать',
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
        description='True — выставлять правила жизненного цикла на префиксы',
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
    schedule=_schedule(),
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
        """🗂 Бакет и сроки хранения по папкам."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            add_note(f"Создан бакет `{BUCKET_NAME}`", context)

        params = context['params']
        if params['dry_run']:
            rows = [f"| `{prefix or '/'}` | {days} |" for prefix, days in _folder_days(params).items()]
            add_note("\n".join(["⚠️ сухой прогон: правила не менялись", "| Папка | Дней |", "|---|---|", *rows]),
                     context, title='Сроки хранения')
            return "Сухой прогон: правила не менялись"
        if not params['lifecycle']:
            return "Правила жизненного цикла не выставлялись: `lifecycle=False`"

        rows, failed = [], []
        for prefix, days in _folder_days(params).items():
            if days <= 0:
                # 0 — «не трогать»; правило с нулём означало бы «удалить всё», и хранилище
                # выполнило бы его само, асинхронно и без отчёта
                continue
            try:
                s3_set_ttl(AWS_CONN_ID, BUCKET_NAME, days=days, prefix=prefix)
            except Exception as e:
                # Правило — не главный механизм: уборку всё равно делает обход ниже.
                # Через корпоративный шлюз PutBucketLifecycleConfiguration раньше отвечал
                # «Missing required header» (см. pg_activity.py), поэтому падать здесь нельзя
                logger.warning(f"⚠️ правило для `{prefix}` не выставлено: {e}")
                failed.append(prefix)
            else:
                rows.append(f"| `{prefix or '/'}` | {days} |")

        note = ["| Папка | Дней |", "|---|---|", *rows]
        if failed:
            note.insert(0, f"⚠️ правила не выставились: {', '.join(f'`{p}`' for p in failed)} — уборка обходом")
        add_note("\n".join(note), context, title='Сроки хранения')
        return f"Правил выставлено {len(rows)}, не вышло {len(failed)}"

    @task
    def sweep(**context):
        """🧹 Обход бакета: статистика по папкам и удаление старше своего срока.

        Обход один на всё: листинг и удаление за проход, а не два прохода подряд — на бакете
        с сотнями тысяч объектов второй листинг стоит столько же, сколько первый (замер на
        стенде 16.09.2026: MinIO отдавал около 100 объектов в секунду, 24 тысячи — четыре
        минуты). Отсюда же потолок по времени: не уложились — отчёт скажет, что данные
        неполные, а слот пула не держим до бесконечности.

        Незнакомые папки листаем, только если им задан срок: осматривать то, что всё равно не
        трогаем, — плата без пользы.
        """
        params = context['params']
        folders = _folder_days(params)
        other_days = _default_days(params)
        now = datetime.now(timezone.utc)
        deadline = time.monotonic() + params['max_minutes'] * 60

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        # Папки бакета, которых нет в карте: их не осматриваем (если не задан other_days), но
        # назвать обязаны — иначе о новой папке никто не узнает
        unknown = sorted(set(_roots(s3_hook)) - {prefix.split('/')[0] + '/' for prefix in folders})
        stats: dict[str, dict] = {}
        batch: list[str] = []
        partial = False

        # Незнакомым папкам срок задан — идём по всему бакету; иначе только по известным.
        # Вложенные папки (отбивки лежат под префиксом логов) обходим раньше родителя: иначе
        # обход родителя выберет весь бюджет времени, а до вложенной очередь не дойдёт
        walks = [''] if other_days else sorted(folders, key=len, reverse=True)
        for walk in walks:
            _, pages = _get_paginator(prefix=walk)
            for page in pages:
                for obj in page.get("Contents") or []:
                    key, size, modified = obj["Key"], obj.get("Size", 0), obj["LastModified"]
                    prefix = _prefix_of(key, folders)
                    # Папки вложены (отбивки лежат под префиксом логов), и при обходе по
                    # префиксам объект попался бы дважды: считаем его в самой длинной папке
                    if walk and prefix != walk:
                        continue
                    days = folders[prefix] if prefix is not None else other_days
                    row = stats.setdefault(
                        prefix if prefix is not None else 'прочее',
                        {'objects': 0, 'bytes': 0, 'oldest': None, 'newest': None,
                         'types': {}, 'days': days, 'over_ttl': 0, 'deleted': 0, 'deleted_bytes': 0},
                    )
                    row['objects'] += 1
                    row['bytes'] += size
                    row['oldest'] = min(row['oldest'] or modified, modified)
                    row['newest'] = max(row['newest'] or modified, modified)
                    suffix = key.rsplit('.', 1)[-1][:8] if '.' in key.rsplit('/', 1)[-1] else 'без типа'
                    row['types'][suffix] = row['types'].get(suffix, 0) + 1

                    if not days or modified > now - timedelta(days=days):
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
                    logger.warning(f"⚠️ обход прерван по времени на `{walk or BUCKET_NAME}`")
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
            'dry_run': bool(params['dry_run']),
            'unknown_folders': unknown,
            'checked_at': now.isoformat(timespec='seconds'),
            'partial': partial,
            'skipped_unknown': not other_days,
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
        over_ttl = {name: row['over_ttl'] for name, row in folders.items() if row['over_ttl'] and row['days']}

        lines = [
            f"`{swept['bucket']}`: {readable_size(total_bytes)}, объектов {readable_size(total_objects, 1000)}, "
            f"удалено {deleted}" + (" (сухой прогон)" if swept.get('dry_run') else ""),
            "",
            "| Папка | Объектов | Объём | Старейший | Дней | Удалено |",
            "|---|---|---|---|---|---|",
        ]
        for name, row in sorted(folders.items(), key=lambda item: -item[1]['objects']):
            oldest = (row['oldest'] or '')[:10]
            lines.append(
                f"| `{name}` | {row['objects']} | {readable_size(row['bytes'])} | {oldest} | "
                f"{row['days'] or '—'} | {row['deleted']} |"
            )
        if swept.get('partial'):
            lines.insert(1, "⚠️ обход не закончен: данные неполные, поднимите `max_minutes` или разберитесь, "
                            "почему листинг медленный")
        unknown = swept.get('unknown_folders') or []
        if unknown:
            lines.append("")
            lines.append(f"Папок вне карты сроков: {', '.join(f'`{name}`' for name in unknown[:10])}")
        if swept.get('skipped_unknown'):
            lines.append("Папки без срока (`other_days=0`) не осматривались.")
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
