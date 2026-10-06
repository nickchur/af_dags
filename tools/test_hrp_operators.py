"""### 🧪 DAG: Регрессионный стенд операторов HRP
*2026-10-05 22:39 MSK · v1.12 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Регрессионный стенд операторов `hrp_operators` на каждый релиз: выгрузки в S3, загрузки из
S3, переливки между БД, утилиты S3, просмотрщики. Цикл setup → операторы → сверка строк и
содержимого → сводка ✅/❌/☮️ в заметке → cleanup. Запуск вручную (`@once`), параллельные
прогоны не поддержаны.

Подробно: [tools/readme.md — test_hrp_operators](../../_plugin_dag_docs/?doc=tools/readme.md#test_hrp_operatorspytest_hrp_operatorspy)
"""

# ruff: noqa: E402  — операторные импорты идут после sys.path-бутстрапа для локальной разработки.
import datetime as dt
import io
import os
import sys
from decimal import Decimal
from logging import getLogger

# Пути для локальной разработки. В продакшне пакет устанавливается через pip,
# поэтому переменные не задаются и sys.path не изменяется.
for _env_var in ("HRP_ETL_CORE_PATH", "HRP_OPERATORS_SRC_PATH"):
    _path = os.environ.get(_env_var)
    if _path:
        sys.path.insert(0, _path)

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import TaskInstance
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.session import create_session
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

from sber_app_dataplatform_etl_core.hrp_operators.click_to_click import HrpClickhouseToClickhouseOperator
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_cluster_operator import HrpClickHouseClusterOperator

# Операторы под тестом — импортируем из конкретных модулей (не через deprecated __getattr__).
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_to_postgres import (
    HrpClickhouseToPostgresIncarnationOperator,
    HrpClickhouseToPostgresOperator,
)
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_to_s3 import (
    HrpClickhouseQueryToS3Operator,
    HrpClickhouseTableToS3Operator,
    HrpClickNativeToS3ListOperator,
    HrpClickNativeToS3Operator,
)
from sber_app_dataplatform_etl_core.hrp_operators.postgres_ddl import HrpPostgresDDL
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_clickhouse import HrpPostgresToClickhouseOperator
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_postgres import (
    HrpPostgresIncarnationInsertOperator,
    HrpPostgresToPostgresOperator,
)
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_s3 import (
    HrpPostgresToS3ListOperator,
    HrpPostgresToS3Operator,
)
from sber_app_dataplatform_etl_core.hrp_operators.s3_archive import HrpS3ArchiveOperator
from sber_app_dataplatform_etl_core.hrp_operators.s3_file_hash import HrpCheckS3FileHash
from sber_app_dataplatform_etl_core.hrp_operators.s3_to_clickhouse import (
    HrpS3ToClickhouseTableOperator,
    HrpS3ToClickhouseTransformedOperator,
)
from sber_app_dataplatform_etl_core.hrp_operators.s3_to_postgres import HrpS3ToPostgresOperator2
from sber_app_dataplatform_etl_core.hrp_operators.s3_to_s3 import HrpS3ToS3Operator
from sber_app_dataplatform_etl_core.hrp_operators.s3_viewer_operator import (
    HrpS3BucketViewerOperator,
    HrpS3FileReadOperator,
    HrpS3ListKeysOperator,
)

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, env_stand, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, env_stand, on_callback  # type: ignore

logger = getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# ───────────────────────────────── Настройки ──────────────────────────────────
DEFAULT_PG_CONN = "airflowdb"
DEFAULT_CH_CONN = "dlab-click"
DEFAULT_S3_CONN = "s3-archive"
# В тикете HRPDATALAB-14479 — test_operators, но «_» в имени бакета S3 недопустим (MinIO стенда
# отказывает: «Bucket name contains invalid characters»)
DEFAULT_S3_BUCKET = "test-operators"
S3_PREFIX = "hrp_tests/"

PG_SCHEMA = "main"
CH_SCHEMA = "technical"

# Стенд — env_stand(): ENV_STAND, при её отсутствии ENVIRONMENT, как во всём tools/ (до
# 29.09.2026 читалась одна ENVIRONMENT). Только на DEV стенд владеет схемой и управляет DDL
# (DROP+CREATE в setup, DROP в cleanup). На прочих стендах таблицы предсозданы
# вручную — setup лишь TRUNCATE+наполняет, cleanup лишь TRUNCATE (не удаляет).
ENV_STAND = env_stand()
IS_DEV = ENV_STAND == "DEV"

# Имена объектов (фиксированы; max_active_runs=1, cleanup гарантирует отсутствие мусора)
SRC = "hrp_src"                  # источник истины
T_PG_TO_PG = "hrp_pg_to_pg"      # таргет PostgresToPostgres
T_CH_TO_PG = "hrp_ch_to_pg"      # таргет ClickhouseToPostgres
T_PG_TO_CH = "hrp_pg_to_ch"      # таргет PostgresToClickhouse (all-String)
T_S3_TO_CH = "hrp_s3_to_ch"      # landing PG→S3→CH (CSV, all-String)
T_S3_TO_CH_LIST = "hrp_s3_to_ch_list"  # landing PG→S3List→CH (TSV, all-String)
T_PG_INC = "hrp_pg_inc"          # таргет PostgresIncarnationInsert (_0/_1 + seq)
T_CH_TO_PG_INC = "hrp_ch_to_pg_inc"    # таргет ClickhouseToPostgresIncarnation (_0/_1 + seq)
T_S3_TO_PG = "hrp_s3_to_pg"      # таргет S3ToPostgres2 (_0/_1 + seq, без колонки incarnation)
T_S3_TO_CH_TR = "hrp_s3_to_ch_tr"  # landing S3ToClickhouseTransformed (file_name + all-String)
T_CH_TO_CH = "hrp_ch_to_ch"      # таргет ClickhouseToClickhouse (типы как у источника)
PROBE = "hrp_setup_probe"        # временный объект setup-пробы прав (create/write/drop)

# Сжатия и их расширения / метки для task_id
COMPRESSIONS_FULL = ["gzip", "zip", "tar.gz", None]
COMPRESSIONS_PG_BASE = ["gzip", None]
_CLABEL = {"gzip": "gzip", "zip": "zip", "tar.gz": "targz", None: "none"}
_COMP_EXT = {"gzip": ".gz", "zip": ".zip", "tar.gz": ".tar.gz", None: ""}


def clabel(c):
    return _CLABEL[c]


def s3key(name, c, ext=".csv"):
    """Ключ S3 для теста name, формата ext (.csv/.json) и сжатия c (с корректным расширением)."""
    return f"{S3_PREFIX}{name}_{clabel(c)}{ext}{_COMP_EXT[c]}"


def _s3_purge_prefix(s3_hook, bucket: str) -> int:
    """Best-effort очистка ключей под S3_PREFIX; возвращает их число (−1 если листинг недоступен).

    Требует s3:ListBucket. Если листинг запрещён (AccessDenied) или падает — НЕ роняем таск:
    purge не критичен (идемпотентность setup_s3 / уборка cleanup с trigger_rule=ALL_DONE),
    объектные проверки стенда всё равно валидируют реальный доступ. Общий для setup_s3/cleanup.
    """
    try:
        keys = s3_hook.list_keys(bucket_name=bucket, prefix=S3_PREFIX)
    except Exception as e:
        logger.warning("S3 purge пропущен: листинг под %s недоступен (нужен s3:ListBucket?): %s",
                       S3_PREFIX, e)
        return -1
    if keys:
        # ВНИМАНИЕ: у S3Hook delete_objects принимает `bucket`, а не `bucket_name` (в отличие
        # от list_keys/load_string/check_for_bucket) — непоследовательность API провайдера.
        s3_hook.delete_objects(bucket=bucket, keys=keys)
    return len(keys or [])


def _skip_setup(system: str, reason: str, context):
    """Пишет причину недоступности/нехватки прав в заметку таска (add_note) и скипает систему.

    Флаг test_* гаснет через AirflowSkipException (setup → ☮️, зависимые проверки → ☮️),
    но причина остаётся видимой в UI таска, а не только в логах.
    """
    add_note(reason, context, level="task", title=f"⚠️ {system}: setup пропущен")
    raise AirflowSkipException(reason)


def _skip_or_retry(system: str, reason: str, exc: Exception, context):
    """Ошибка пробы: пока есть ретраи — пробрасываем (transient «Connection reset by peer»
    переживаем через retries), когда исчерпаны — трактуем как недоступность и скипаем флаг.

    Так транзиентный сетевой сбой не гасит систему преждевременно, а устойчивая
    недоступность/нехватка прав всё равно уводит проверки в ☮️ (а не в ❌ каскадом).
    """
    ti = context["ti"]
    if ti.is_eligible_to_retry():
        raise exc
    _skip_setup(system, reason, context)


# ───────────────────────── Единый источник данных ─────────────────────────────
# (name, pg_type, ch_type_typed, ch_type_string, comment) — комменты обязательны для QG.
COLUMNS = [
    ("id",          "integer",          "Int32",                    "String", "Первичный ключ строки"),
    ("name",        "text",             "String",                   "String", "Обычная строка без спецсимволов"),
    ("s_special",   "text",             "String",                   "String", "Строка со спецсимволами и кириллицей"),
    ("val_int",     "integer",          "Nullable(Int32)",          "String", "Целое число, nullable"),
    ("val_bigint",  "bigint",           "Nullable(Int64)",          "String", "Большое целое число, nullable"),
    ("val_float",   "double precision", "Nullable(Float64)",        "String", "Число с плавающей точкой, nullable"),
    ("val_numeric", "numeric(18,4)",    "Nullable(Decimal(18,4))",  "String", "Десятичное число, nullable"),
    ("val_bool",    "boolean",          "Nullable(UInt8)",          "String", "Булев флаг, nullable"),
    ("arr_int",     "integer[]",        "Array(Int32)",             "String", "Массив целых чисел"),
    ("arr_text",    "text[]",           "Array(String)",            "String", "Массив строк"),
    ("val_date",    "date",             "Nullable(Date)",           "String", "Календарная дата, nullable"),
    ("val_ts",      "timestamp",        "Nullable(DateTime)",       "String", "Отметка времени, nullable"),
    ("val_tstz",    "timestamptz",      "Nullable(DateTime64(3, 'UTC'))", "String", "Отметка времени с поясом и миллисекундами, nullable"),
    ("val_uuid",    "uuid",             "Nullable(UUID)",           "String", "UUID, nullable"),
    ("val_json",    "jsonb",            "Nullable(String)",         "String", "JSON-документ, nullable"),
    ("arr_null",    "text[]",           "Array(Nullable(String))",  "String", "Массив строк с NULL внутри"),
    ("val_lc",      "text",             "LowCardinality(String)",   "String", "Строка-справочник (LowCardinality в CH)"),
]
COL_NAMES = [c[0] for c in COLUMNS]

_SPECIAL = 'Спец: "кавычки", запятая, ; точка-с-запятой,\tтаб,\nперенос, \\ бэкслеш, №42'

# Три строки: обычная, со спецсимволами/массивами, полностью NULL (кроме id).
ROWS = [
    (1, "Normal row", "ascii-only text", 123, 1234567890123, 1.23, Decimal("1.2300"),
     True, [1, 2, 3], ["a", "b", "c"], dt.date(2023, 1, 1), dt.datetime(2023, 1, 1, 12, 0, 0),
     dt.datetime(2023, 1, 1, 12, 0, 0, 123000, tzinfo=dt.timezone.utc), "0b0f5bb4-5a3e-4c1e-9d6b-1f2a3b4c5d6e",
     '{"k": [1, 2], "s": "a"}', ["x", None], "alpha"),
    (2, "Special row", _SPECIAL, 456, None, 4.56, Decimal("4.5600"),
     False, [4, 5, 6], ["спец", "b,c", "x;y"], dt.date(2023, 1, 2), dt.datetime(2023, 1, 2, 13, 30, 0),
     dt.datetime(2023, 1, 2, 13, 30, 0, 456000, tzinfo=dt.timezone.utc), "c3d4e5f6-0000-4000-8000-000000000002",
     '{"q": "спец; \\"кавычки\\""}', [None, "b,c"], "beta"),
    # LowCardinality(String) в CH не nullable — пустая строка, а не NULL
    (3, "Nulls row", "", None, None, None, None,
     None, [], [], None, None, None, None, None, [], ""),
]
# Строка 2 после xstream_sanitize (XSTREAM_CLEAN ядра): вырезаны «;», таб, перенос и обратный слеш.
# Литерал, а не вызов регулярки ядра: проверка не должна доверять проверяемому коду
SANITIZED_SPECIAL = 'Спец: "кавычки", запятая,  точка-с-запятой,таб,перенос,  бэкслеш, №42'
EXPECTED_ROWS = len(ROWS)


def _ch_lit(v) -> str:
    """Литерал значения для ClickHouse INSERT ... VALUES."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, Decimal)):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, dt.datetime):
        # с поясом — DateTime64(3): миллисекунды; без — DateTime: до секунд
        fmt = "%Y-%m-%d %H:%M:%S.%f" if v.tzinfo else "%Y-%m-%d %H:%M:%S"
        return "'" + v.strftime(fmt)[:23] + "'"
    if isinstance(v, dt.date):
        return "'" + v.strftime("%Y-%m-%d") + "'"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_ch_lit(x) for x in v) + "]"
    s = (str(v).replace("\\", "\\\\").replace("'", "\\'")
         .replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r"))
    return "'" + s + "'"


def _pg_create_sql(table: str, incarnation: bool = False, inc_col: bool = True) -> str:
    """CREATE TABLE + COMMENT (таблица и каждая колонка) для Quality Gate.

    incarnation=True создаёт _0/_1 + последовательность; inc_col — с ведущей колонкой
    incarnation (её пишут Incarnation-операторы; S3ToPostgres2 копирует файл как есть — без неё).
    """
    full = f"{PG_SCHEMA}.{table}"
    if not IS_DEV:
        # Не-DEV: таблицы предсозданы — DDL не трогаем, только очищаем.
        if incarnation:
            return f"TRUNCATE TABLE {full}_0;\nTRUNCATE TABLE {full}_1;"
        return f"TRUNCATE TABLE {full};"
    cols = []
    comments = []
    if incarnation and inc_col:
        cols.append("incarnation integer")
        comments.append("COMMENT ON COLUMN {tbl}.incarnation IS 'Номер инкарнации (0/1)';")
    for name, pg_type, *_rest, comment in COLUMNS:
        pk = " PRIMARY KEY" if (name == "id" and not incarnation) else ""
        cols.append(f"{name} {pg_type}{pk}")
        comments.append(f"COMMENT ON COLUMN {{tbl}}.{name} IS '{comment}';")
    cols_sql = ",\n    ".join(cols)

    if not incarnation:
        stmts = [f"DROP TABLE IF EXISTS {full} CASCADE;",
                 f"CREATE TABLE {full} (\n    {cols_sql}\n);",
                 f"COMMENT ON TABLE {full} IS 'HRP regression: {table}';"]
        stmts += [c.format(tbl=full) for c in comments]
        return "\n".join(stmts)

    # Инкарнационный таргет: две физические таблицы + последовательность.
    stmts = [f"DROP TABLE IF EXISTS {full}_0 CASCADE;",
             f"DROP TABLE IF EXISTS {full}_1 CASCADE;",
             f"DROP SEQUENCE IF EXISTS {full}_inc_seq;",
             f"CREATE SEQUENCE {full}_inc_seq START WITH 0 MINVALUE 0;"]
    for suffix in ("0", "1"):
        phys = f"{full}_{suffix}"
        stmts.append(f"CREATE TABLE {phys} (\n    {cols_sql}\n);")
        stmts.append(f"COMMENT ON TABLE {phys} IS 'HRP regression incarnation {table}_{suffix}';")
        stmts += [c.format(tbl=phys) for c in comments]
    return "\n".join(stmts)


def _ch_create_sql(table: str, typed: bool, lead: str = "") -> str:
    """CREATE TABLE для ClickHouse. typed=True — реальные типы (Array/Nullable), иначе all-String.

    lead — колонки перед колонками источника (S3ToClickhouseTransformed вставляет file_name первой).
    """
    full = f"{CH_SCHEMA}.{table}"
    if not IS_DEV:
        # Не-DEV: таблица предсоздана — только очищаем.
        return f"TRUNCATE TABLE {full};"
    idx = 1 if typed else 2  # позиция типа в COLUMNS
    cols = ",\n    ".join([lead] * bool(lead) + [f"{c[0]} {c[idx + 1]}" for c in COLUMNS])
    order = "id" if typed else "tuple()"
    return (f"DROP TABLE IF EXISTS {full};\n"
            f"CREATE TABLE {full} (\n    {cols}\n) ENGINE = MergeTree() ORDER BY {order};")


def _ch_insert_sql(table: str) -> str:
    values = ",\n".join("(" + ", ".join(_ch_lit(v) for v in row) + ")" for row in ROWS)
    return f"INSERT INTO {CH_SCHEMA}.{table} VALUES\n{values}"


# ──────────────────────────────── DAG ─────────────────────────────────────────
@dag(
    dag_id="test_hrp_operators",
    schedule="@once",
    start_date=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(hours=2),
    # pg_export — политика etl-core 1.2.5 (task_policy): даг с ClickHouse → Postgres без тега не импортируется
    tags=["DataTools", "tools", "AutoQA", "pg_export"],
    # retries — переживаем transient «Connection reset by peer» от общего ClickHouse/S3
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 2,
        "retry_delay": dt.timedelta(seconds=30),
        # Тестовые таблицы — десятки строк: потолок ловит зависание, а не объём
        "execution_timeout": dt.timedelta(minutes=30),
        # Единый вывод исхода в заметку КАЖДОГО таска (успех/скип-причина/ошибка-причина).
        # on_callback пишет add_note со state (✅/❌/SKIPPED) и текстом context['exception'].
        # on_skipped_callback (Airflow ≥2.9) срабатывает на AirflowSkipException — так причина
        # скипа setup_* (флаг/недоступность) попадает в заметку и без явного add_note.
        "on_success_callback": on_callback,
        "on_failure_callback": on_callback,
        "on_skipped_callback": on_callback,
    },
    params={
        "pg_conn_id": Param(DEFAULT_PG_CONN, type="string", description="Postgres для тестовых таблиц (схема public)"),
        "ch_conn_id": Param(DEFAULT_CH_CONN, type="string", description="ClickHouse для тестовых таблиц (схема technical)"),
        "s3_conn_id": Param(DEFAULT_S3_CONN, type="string", description="S3-подключение для тестовых файлов"),
        "s3_bucket": Param(DEFAULT_S3_BUCKET, type="string", description="Бакет для тестовых файлов (префикс hrp_tests/)"),
        # Флаги систем: каждая проверка гейтуется по системам, которые она задействует (AND).
        # Кросс-системные проверки идут только при включённых ОБЕИХ системах: pg→s3 требует
        # test_pg И test_s3, s3→ch — test_s3 И test_ch, ch→pg — test_ch И test_pg и т.д.
        # Выключение системы уводит все её проверки (в т.ч. кросс) в ☮️ skipped.
        "test_pg": Param(default=True, type="boolean", description="Проверки с Postgres; кросс-системные идут, только если включены обе системы"),
        "test_ch": Param(default=True, type="boolean", description="Проверки с ClickHouse"),
        "test_s3": Param(default=True, type="boolean", description="Проверки с S3"),
        # «Карантин» поверх системных флагов — только дефекты пакета, исправленные в ядре 1.2.7.
        # По умолчанию False (☮️ skipped); после выкладки 1.2.7 на контуры флаг снимается:
        #   pg_to_s3_list  — prepare_row без флагов: next(list) → TypeError;
        #   s3_to_ch_tsv   — зависит от pg_to_s3_list;
        #   ch_native_list — JSON-путь NativeClickhouseStream не сериализует Decimal;
        #   pg_incarnation — insert_incarnation: sql.Literal вместо sql.SQL.
        # Санитизацию (pg_to_s3_list_sanitize, ch_native_to_s3_sanitize) сюда не прячем: флаг
        # xstream_sanitize до 1.2.7 не чистил ничего, а выгрузки ЕР на него полагаются.
        "run_known_broken": Param(default=False, type="boolean", description="Карантин: проверки дефектов пакета, исправленных в ядре 1.2.7"),
        # Не дефект, а окружение: Table/Query→S3 считают строки через clusterAllReplicas(datalab,
        # system.query_log), ClusterOperator — узлы из system.clusters (datalab), на каждый ходит подключением click-dlab-<узел>.
        "run_cluster": Param(default=False, type="boolean", description="Проверки, которым нужен кластер ClickHouse datalab: ch_table/ch_query → S3 и ch_cluster_ddl"),
        # На время отладки: False оставляет все PG/CH таблицы и S3-ключи, чтобы можно было
        # переразобрать/перезапустить отдельный упавший таск (иначе cleanup сносит всё).
        "run_cleanup": Param(default=True, type="boolean", description="Удалить тестовые таблицы и ключи S3 после прогона; выкл. — оставить для отладки"),
    },
    doc_md=__doc__,
    description='Регрессионный стенд операторов hrp_operators: pg, ch, s3',
)
def test_hrp_operators_dag():

    # conn_id/бакет передаём операторам ЛИТЕРАЛАМИ, а не через {{ params }}: часть
    # hrp_operators (PostgresToPostgres, S3ToClickhouseTable, ClickhouseToPostgres, …)
    # не объявляет conn_id/s3_bucket в template_fields, поэтому Jinja у них не рендерится
    # и падает с «conn_id `{{ params.* }}` isn't defined». Дефолты совпадают с params.*,
    # @task-хелперы (setup/validate/cleanup) по-прежнему берут conn_id из params[...] в рантайме.
    PG_CONN = DEFAULT_PG_CONN  # noqa: N806
    CH_CONN = DEFAULT_CH_CONN  # noqa: N806
    S3_CONN = DEFAULT_S3_CONN  # noqa: N806
    BUCKET = DEFAULT_S3_BUCKET  # noqa: N806

    # ─────────────────────────────── setup ────────────────────────────────────
    @task
    def setup_pg(params=None, **context):
        """Создаёт PG-источник и все PG-таргеты (с комментами) + наполняет источник."""
        # Скипается при test_pg=False, либо если PG недоступен / нет прав на create/write (проба
        # гасит test_pg, причина пишется в заметку таска через add_note; проверки уходят в ☮️
        # вместо каскада падений). Ошибка настоящего DDL/insert стенда ПОСЛЕ успешной пробы —
        # реальный fail (сигнал регрессии не маскируется).
        if not params.get("test_pg"):
            raise AirflowSkipException("test_pg=False — PG-проверки отключены, setup_pg пропущен")
        pg = PostgresHook(postgres_conn_id=params["pg_conn_id"])
        # Проба прав: create/insert/drop временного объекта. SELECT 1 недостаточно — коннект
        # может пройти, но не быть прав на создание/запись; тогда это «недоступность», а не fail.
        probe = f"{PG_SCHEMA}.{PROBE}"
        try:
            if IS_DEV:
                pg.run([
                    f"DROP TABLE IF EXISTS {probe}",
                    f"CREATE TABLE {probe} (x integer)",
                    f"INSERT INTO {probe} VALUES (1)",
                    f"DROP TABLE {probe}",
                ])
            else:
                # Не-DEV: DDL-прав может не быть — проверяем лишь доступность коннекта.
                pg.get_first("SELECT 1")
        except Exception as e:
            reason = "PG недоступен / нет прав на create/write" if IS_DEV else "PG недоступен"
            _skip_or_retry("PG", f"{reason} "
                           f"({params['pg_conn_id']!r}): {e} — PG-проверки пропущены", e, context)
        ddl = "\n".join([
            _pg_create_sql(SRC),
            _pg_create_sql(T_PG_TO_PG),
            _pg_create_sql(T_CH_TO_PG),
            _pg_create_sql(T_PG_INC, incarnation=True),
            _pg_create_sql(T_CH_TO_PG_INC, incarnation=True),
            _pg_create_sql(T_S3_TO_PG, incarnation=True, inc_col=False),
        ])
        pg.run(ddl)

        cols = ", ".join(COL_NAMES)
        placeholders = ", ".join(["%s"] * len(COL_NAMES))
        insert = f"INSERT INTO {PG_SCHEMA}.{SRC} ({cols}) VALUES ({placeholders})"
        conn = pg.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(insert, ROWS)
            conn.commit()
        finally:
            conn.close()
        logger.info("Postgres setup complete: %d rows in %s.%s", EXPECTED_ROWS, PG_SCHEMA, SRC)

    @task
    def setup_ch(params=None, **context):
        """Создаёт CH-источник (typed) и landing-таблицы (all-String) + наполняет источник."""
        # Скипается при test_ch=False, либо если CH недоступен / нет прав на create/write (проба
        # гасит test_ch, причина пишется в заметку таска через add_note; проверки уходят в ☮️
        # вместо каскада падений). Ошибка настоящего DDL/insert стенда ПОСЛЕ успешной пробы —
        # реальный fail (сигнал регрессии не маскируется).
        if not params.get("test_ch"):
            raise AirflowSkipException("test_ch=False — CH-проверки отключены, setup_ch пропущен")
        ch = ClickHouseHook(clickhouse_conn_id=params["ch_conn_id"])
        # Проба прав: create/insert/drop временной таблицы. SELECT 1 недостаточно — коннект
        # может пройти, но не быть прав на создание/запись; тогда это «недоступность», а не fail.
        probe = f"{CH_SCHEMA}.{PROBE}"
        try:
            if IS_DEV:
                ch.execute(f"DROP TABLE IF EXISTS {probe}")
                ch.execute(f"CREATE TABLE {probe} (x Int32) ENGINE = MergeTree() ORDER BY x")
                ch.execute(f"INSERT INTO {probe} VALUES (1)")
                ch.execute(f"DROP TABLE {probe}")
            else:
                # Не-DEV: DDL-прав может не быть — проверяем лишь доступность коннекта.
                ch.execute("SELECT 1")
        except Exception as e:
            reason = "CH недоступен / нет прав на create/write" if IS_DEV else "CH недоступен"
            _skip_or_retry("CH", f"{reason} "
                           f"({params['ch_conn_id']!r}): {e} — CH-проверки пропущены", e, context)
        ddl = [_ch_create_sql(t, typed=True) for t in (SRC, T_CH_TO_CH)]
        ddl += [_ch_create_sql(t, typed=False) for t in (T_PG_TO_CH, T_S3_TO_CH, T_S3_TO_CH_LIST)]
        ddl.append(_ch_create_sql(T_S3_TO_CH_TR, typed=False, lead="file_name String"))
        for stmt in ";".join(ddl).split(";"):
            if stmt.strip():
                ch.execute(stmt)
        ch.execute(_ch_insert_sql(SRC))
        logger.info("ClickHouse setup complete: %d rows in %s.%s", EXPECTED_ROWS, CH_SCHEMA, SRC)

    @task
    def setup_s3(params=None, **context):
        """Проверяет S3-соединение и существование бакета + чистит префикс от прошлых прогонов."""
        # Аналог DROP→CREATE у setup_pg/setup_ch: делает S3 идемпотентным, чтобы стейл-ключи
        # (например после прогона с run_cleanup=False) не искажали s3_list_keys/bucket_viewer.
        # Скипается при test_s3=False, либо если S3 недоступен / нет бакета / нет прав на запись
        # (проба гасит test_s3, причина пишется в заметку таска через add_note; проверки уходят
        # в ☮️ вместо каскада падений).
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        if not params.get("test_s3"):
            raise AirflowSkipException("test_s3=False — S3-проверки отключены, setup_s3 пропущен")
        bucket = params["s3_bucket"]
        s3 = S3Hook(aws_conn_id=params["s3_conn_id"])
        # Проба прав: существование бакета + put/delete временного ключа. check_for_bucket
        # проверяет только доступ на чтение — записи прав может не быть; тогда это «недоступность».
        probe_key = f"{S3_PREFIX}{PROBE}"
        try:
            available = s3.check_for_bucket(bucket)
            if available:
                s3.load_string("probe", key=probe_key, bucket_name=bucket, replace=True)
                s3.delete_objects(bucket=bucket, keys=[probe_key])
        except Exception as e:
            _skip_or_retry("S3", f"S3 недоступен / нет прав на запись "
                           f"({params['s3_conn_id']!r}): {e} — S3-проверки пропущены", e, context)
        if not available:
            _skip_setup("S3", f"S3 бакет {bucket!r} недоступен "
                        f"(conn={params['s3_conn_id']!r}) — S3-проверки пропущены", context)
        removed = _s3_purge_prefix(s3, bucket)
        purged = "листинг недоступен" if removed < 0 else f"удалено {removed} стейл-ключей"
        logger.info("S3 setup complete: бакет %s доступен, %s под %s", bucket, purged, S3_PREFIX)

    # ───────────────────────── validation helpers ─────────────────────────────
    @task
    def validate_ch_count(table: str, expected: int = EXPECTED_ROWS, params=None):
        """Сверяет число строк в таблице ClickHouse с ожидаемым."""
        ch = ClickHouseHook(clickhouse_conn_id=params["ch_conn_id"])
        cnt = ch.execute(f"SELECT count() FROM {CH_SCHEMA}.{table}")[0][0]
        if cnt != expected:
            raise AirflowFailException(f"{CH_SCHEMA}.{table}: ожидалось {expected} строк, получено {cnt}")
        logger.info("OK: %s.%s = %d строк", CH_SCHEMA, table, cnt)

    @task
    def validate_pg_to_pg(params=None):
        """Полная сверка содержимого: симметричный EXCEPT должен быть пуст."""
        pg = PostgresHook(postgres_conn_id=params["pg_conn_id"])
        src, tgt = f"{PG_SCHEMA}.{SRC}", f"{PG_SCHEMA}.{T_PG_TO_PG}"
        a = pg.get_first(f"SELECT count(*) FROM (SELECT * FROM {src} EXCEPT SELECT * FROM {tgt}) d")[0]
        b = pg.get_first(f"SELECT count(*) FROM (SELECT * FROM {tgt} EXCEPT SELECT * FROM {src}) d")[0]
        if a or b:
            raise AirflowFailException(f"pg_to_pg: содержимое отличается (src→tgt={a}, tgt→src={b})")
        logger.info("OK: pg_to_pg содержимое идентично источнику")

    @task
    def validate_ch_to_pg(params=None):
        """Сверка count и инварианта sum(id) для ClickhouseToPostgres."""
        pg = PostgresHook(postgres_conn_id=params["pg_conn_id"])
        cnt, sid = pg.get_first(f"SELECT count(*), coalesce(sum(id),0) FROM {PG_SCHEMA}.{T_CH_TO_PG}")
        if cnt != EXPECTED_ROWS or sid != 6:
            raise AirflowFailException(f"ch_to_pg: count={cnt} (ждём {EXPECTED_ROWS}), sum(id)={sid} (ждём 6)")
        logger.info("OK: ch_to_pg count=%d sum(id)=%d", cnt, sid)

    @task
    def validate_incarnation(table: str, params=None):
        """Проверяет, что активная инкарнация таргета содержит EXPECTED_ROWS строк."""
        from contextlib import closing

        from sber_app_dataplatform_etl_core.hrp_operators.utils.pg_utils import current_incarnation
        pg = PostgresHook(postgres_conn_id=params["pg_conn_id"])
        with closing(pg.get_conn()) as conn:
            active = current_incarnation(conn, PG_SCHEMA, table)
        cnt = pg.get_first(f"SELECT count(*) FROM {PG_SCHEMA}.{table}_{active}")[0]
        if cnt != EXPECTED_ROWS:
            raise AirflowFailException(f"{table}: активная инкарнация {active} содержит {cnt} строк")
        logger.info("OK: %s активная инкарнация %s = %d строк", table, active, cnt)

    @task
    def validate_sanitized(key: str, params=None):
        """Сверяет выгрузку с xstream_sanitize: строка 2 очищена, массивы тоже, строк столько же."""
        import csv
        import gzip

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        raw = S3Hook(aws_conn_id=params["s3_conn_id"]).get_key(key, params["s3_bucket"]).get()["Body"].read()
        # Формат выгрузок по умолчанию: TSV, QUOTE_NONE, экранирование обратным слешем, заголовок
        rows = list(csv.reader(io.StringIO(gzip.decompress(raw).decode()), delimiter="\t",
                               quoting=csv.QUOTE_NONE, escapechar="\\"))
        header, data = rows[0], rows[1:]
        if len(data) != EXPECTED_ROWS:
            raise AirflowFailException(f"{key}: {len(data)} строк, ждём {EXPECTED_ROWS}")
        row = dict(zip(header, next(r for r in data if r[0] == "2")))
        errors = []
        if row["s_special"] != SANITIZED_SPECIAL:
            errors.append(f"s_special={row['s_special']!r}, ждём {SANITIZED_SPECIAL!r}")
        if "x;y" in row["arr_text"] or "xy" not in row["arr_text"]:
            errors.append(f"arr_text не очищен (sanitize_array): {row['arr_text']!r}")
        if errors:
            raise AirflowFailException(f"{key}: " + "; ".join(errors))
        logger.info("OK: %s очищен, %d строк", key, len(data))

    # TaskGroup'ы используем с prefix_group_id=False: task_id остаются прежними (их
    # используют XCom-pull'ы, ветки-гейты и report), меняется лишь группировка в UI.

    # ─────────────────────────────── setup ────────────────────────────────────
    with TaskGroup(group_id="setup", prefix_group_id=False):
        setup_pg_t = setup_pg()
        setup_ch_t = setup_ch()
        setup_s3_t = setup_s3()

    exports = []

    # Гейты групп: на каждую группу — branch по условию над системными флагами params.
    # Условие = все задействованные оператором системы включены (AND); битые проверки
    # дополнительно требуют run_known_broken. При False вся группа (ops + каскадом их
    # валидаторы) скипается. Собираем гейты для report/all_tasks.
    gates = []

    def gate_cond(*flags):
        """Условие гейта: все перечисленные флаги params должны быть True."""
        return lambda p: all(p.get(f) for f in flags)

    def make_gate(cond, ops, gate_id, upstream=None):
        ids = [o.task_id for o in ops]

        @task.branch(task_id=gate_id)  # gate_id всегда с префиксом "gate_" (report их исключает)
        def _gate(params=None):
            """Пропускает группу проверок, если её флаг в параметрах выключен."""
            return ids if cond(params) else []

        g = _gate()
        if upstream is not None:
            upstream >> g
        g >> ops
        gates.append(g)
        return g

    # Группы to_s3 (раздельные гейты, чтобы изолировать карантинные проверки под run_known_broken).
    pg_to_s3_ops, pg_to_s3_list_ops = [], []
    ch_native_ops, ch_native_list_ops, ch_table_query_ops = [], [], []

    # ───────────────────────────── to_s3 ──────────────────────────────────────
    # Каждый to-S3 оператор с post_file_check=True сам перечитывает файл и сверяет хэш,
    # поэтому успех таска = пройденная проверка целостности для данного сжатия.
    with TaskGroup(group_id="to_s3", prefix_group_id=False):
        for c in COMPRESSIONS_PG_BASE:
            op = HrpPostgresToS3Operator(
                task_id=f"pg_to_s3_{clabel(c)}",
                table_name=SRC, schema=PG_SCHEMA,
                s3_bucket=BUCKET, s3_key=s3key("pg_to_s3", c),
                postgres_conn_id=PG_CONN, aws_conn_id=S3_CONN,
                compression=c, replace=True, post_file_check=True,
            )
            [setup_pg_t, setup_s3_t] >> op
            exports.append(op)
            pg_to_s3_ops.append(op)

        for c in COMPRESSIONS_FULL:
            op = HrpPostgresToS3ListOperator(
                task_id=f"pg_to_s3_list_{clabel(c)}",
                table_name=SRC, schema=PG_SCHEMA,
                s3_bucket=BUCKET, s3_key=s3key("pg_to_s3_list", c),
                postgres_conn_id=PG_CONN, aws_conn_id=S3_CONN,
                compression=c, replace=True, post_file_check=True, header=True,
            )
            [setup_pg_t, setup_s3_t] >> op
            exports.append(op)
            pg_to_s3_list_ops.append(op)

        # xstream_sanitize: тот же источник, но строка 2 и её массив обязаны выйти очищенными
        pg_to_s3_sanitize = HrpPostgresToS3ListOperator(
            task_id="pg_to_s3_list_sanitize",
            table_name=SRC, schema=PG_SCHEMA,
            s3_bucket=BUCKET, s3_key=s3key("pg_to_s3_sanitize", "gzip"),
            postgres_conn_id=PG_CONN, aws_conn_id=S3_CONN,
            compression="gzip", replace=True, post_file_check=True, header=True,
            xstream_sanitize=True, sanitize_array=True,
        )
        v_pg_sanitize = validate_sanitized.override(task_id="v_pg_to_s3_sanitize")(
            "{{ ti.xcom_pull(task_ids='pg_to_s3_list_sanitize', key='s3_key_list')[0] }}")
        [setup_pg_t, setup_s3_t] >> pg_to_s3_sanitize >> v_pg_sanitize
        exports.append(pg_to_s3_sanitize)

        ch_native_sanitize = HrpClickNativeToS3Operator(
            task_id="ch_native_to_s3_sanitize",
            sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
            s3_bucket=BUCKET, s3_key=s3key("ch_native_sanitize", "gzip"),
            clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
            compression="gzip", replace=True, post_file_check=True, fmt="CSV",
            xstream_sanitize=True, sanitize_array=True,
        )
        v_ch_sanitize = validate_sanitized.override(task_id="v_ch_native_sanitize")(
            s3key("ch_native_sanitize", "gzip"))
        [setup_ch_t, setup_s3_t] >> ch_native_sanitize >> v_ch_sanitize
        exports.append(ch_native_sanitize)

        for c in COMPRESSIONS_FULL:
            op = HrpClickNativeToS3Operator(
                task_id=f"ch_native_to_s3_{clabel(c)}",
                sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
                s3_bucket=BUCKET, s3_key=s3key("ch_native", c),
                clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
                compression=c, replace=True, post_file_check=True, fmt="CSV",
            )
            [setup_ch_t, setup_s3_t] >> op
            exports.append(op)
            ch_native_ops.append(op)

        for c in COMPRESSIONS_FULL:
            op = HrpClickNativeToS3ListOperator(
                task_id=f"ch_native_list_{clabel(c)}",
                sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
                s3_bucket=BUCKET, s3_key=s3key("ch_native_list", c, ext=".json"),
                clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
                compression=c, replace=True, post_file_check=True, fmt="JSON",
            )
            [setup_ch_t, setup_s3_t] >> op
            exports.append(op)
            ch_native_list_ops.append(op)

        # ⚠ Table/Query→S3 считают строки через clusterAllReplicas(datalab, system.query_log).
        for c in COMPRESSIONS_FULL:
            op = HrpClickhouseTableToS3Operator(
                task_id=f"ch_table_to_s3_{clabel(c)}",
                table_name=SRC, schema=CH_SCHEMA,
                s3_bucket=BUCKET, s3_key=s3key("ch_table", c),
                clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
                compression=c, replace=True, post_file_check=True,
            )
            [setup_ch_t, setup_s3_t] >> op
            exports.append(op)
            ch_table_query_ops.append(op)

        ch_query_to_s3 = HrpClickhouseQueryToS3Operator(
            task_id="ch_query_to_s3_gzip",
            sql=f"SELECT id, name FROM {CH_SCHEMA}.{SRC} WHERE id > 1",
            s3_bucket=BUCKET, s3_key=s3key("ch_query", "gzip"),
            clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
            compression="gzip", replace=True, post_file_check=True,
        )
        [setup_ch_t, setup_s3_t] >> ch_query_to_s3
        exports.append(ch_query_to_s3)
        ch_table_query_ops.append(ch_query_to_s3)

        make_gate(gate_cond("test_pg", "test_s3"), pg_to_s3_ops, "gate_pg_to_s3")
        make_gate(gate_cond("run_known_broken", "test_pg", "test_s3"), pg_to_s3_list_ops, "gate_pg_to_s3_list")
        make_gate(gate_cond("test_ch", "test_s3"), ch_native_ops, "gate_ch_native")
        make_gate(gate_cond("run_known_broken", "test_ch", "test_s3"), ch_native_list_ops, "gate_ch_native_list")
        make_gate(gate_cond("run_cluster", "test_ch", "test_s3"), ch_table_query_ops, "gate_ch_table_query")
        make_gate(gate_cond("test_pg", "test_s3"), [pg_to_s3_sanitize], "gate_pg_to_s3_sanitize")
        make_gate(gate_cond("test_ch", "test_s3"), [ch_native_sanitize], "gate_ch_native_sanitize")

    # ───────────────────────── s3_to_db (end-to-end) ──────────────────────────
    # Перезаливаем gzip-выгрузки обратно в CH и сверяем row count.
    pg_to_s3_gzip = next(o for o in exports if o.task_id == "pg_to_s3_gzip")
    pg_to_s3_list_gzip = next(o for o in exports if o.task_id == "pg_to_s3_list_gzip")

    with TaskGroup(group_id="s3_to_db", prefix_group_id=False):
        s3_to_ch_csv = HrpS3ToClickhouseTableOperator(
            task_id="s3_to_ch_csv",
            s3_bucket=BUCKET, s3_key=s3key("pg_to_s3", "gzip"),
            clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
            table_name=T_S3_TO_CH, schema=CH_SCHEMA,
            fmt="CSV", compression="gzip", truncate=True,
        )
        # HrpPostgresToS3ListOperator пишет файлы с номерным суффиксом (..._1.csv.gz),
        # поэтому берём фактический ключ из XCom (s3_key_list), а не вычисляем литерально.
        s3_to_ch_tsv = HrpS3ToClickhouseTableOperator(
            task_id="s3_to_ch_tsv",
            s3_bucket=BUCKET,
            s3_key="{{ ti.xcom_pull(task_ids='pg_to_s3_list_gzip', key='s3_key_list')[0] }}",
            clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
            table_name=T_S3_TO_CH_LIST, schema=CH_SCHEMA,
            fmt="TSVWithNames", compression="gzip", truncate=True,
        )
        v_s3_csv = validate_ch_count.override(task_id="v_s3_to_ch_csv")(T_S3_TO_CH)
        v_s3_tsv = validate_ch_count.override(task_id="v_s3_to_ch_tsv")(T_S3_TO_CH_LIST)
        [setup_ch_t, pg_to_s3_gzip] >> s3_to_ch_csv >> v_s3_csv
        [setup_ch_t, pg_to_s3_list_gzip] >> s3_to_ch_tsv >> v_s3_tsv

        make_gate(gate_cond("test_s3", "test_ch"), [s3_to_ch_csv], "gate_s3_to_ch_csv")
        make_gate(gate_cond("run_known_broken", "test_s3", "test_ch"), [s3_to_ch_tsv], "gate_s3_to_ch_tsv")

        # S3ToPostgres2 копирует файл через COPY … CSV QUOTE <copy_csv_quote>. Выгрузка
        # PostgresToS3 — родной CSV Postgres (кавычки "), поэтому ровно его и отдаём.
        s3_to_pg = HrpS3ToPostgresOperator2(
            task_id="s3_to_pg",
            s3_bucket=BUCKET, s3_key=s3key("pg_to_s3", "gzip"), aws_conn_id=S3_CONN,
            target_conn_id=PG_CONN, target_schema=PG_SCHEMA, target_table=T_S3_TO_PG,
            compression="gzip", copy_csv_quote='"',
        )
        v_s3_to_pg = validate_incarnation.override(task_id="v_s3_to_pg")(T_S3_TO_PG)
        [setup_pg_t, pg_to_s3_gzip] >> s3_to_pg >> v_s3_to_pg

        # Transformed читает TSV без экранирования (формат пакетной выгрузки в ПКАП): табы и
        # переводы строк внутри значений ему противопоказаны — берём очищенную выгрузку CH.
        s3_to_ch_tr = HrpS3ToClickhouseTransformedOperator(
            task_id="s3_to_ch_transformed",
            s3_bucket=BUCKET, s3_key=s3key("ch_native_sanitize", "gzip"),
            clickhouse_conn_id=CH_CONN, aws_conn_id=S3_CONN,
            table_name=T_S3_TO_CH_TR, schema=CH_SCHEMA,
            fmt="TabSeparatedWithNames", compression="gzip", truncate=True,
            include_file_name=True, file_name_position=0,
        )
        v_s3_to_ch_tr = validate_ch_count.override(task_id="v_s3_to_ch_transformed")(T_S3_TO_CH_TR)
        [setup_ch_t, ch_native_sanitize] >> s3_to_ch_tr >> v_s3_to_ch_tr

        make_gate(gate_cond("test_s3", "test_pg"), [s3_to_pg], "gate_s3_to_pg")
        make_gate(gate_cond("test_s3", "test_ch"), [s3_to_ch_tr], "gate_s3_to_ch_transformed")

    # ──────────────────────────── db_to_db ────────────────────────────────────
    with TaskGroup(group_id="db_to_db", prefix_group_id=False):
        pg_to_pg = HrpPostgresToPostgresOperator(
            task_id="pg_to_pg",
            source_conn_id=PG_CONN, source_schema=PG_SCHEMA, source_table=SRC,
            target_conn_id=PG_CONN, target_schema=PG_SCHEMA, target_table=T_PG_TO_PG,
            truncate=True, post_count_check=True,
        )
        v_pg_to_pg = validate_pg_to_pg()
        setup_pg_t >> pg_to_pg >> v_pg_to_pg

        ch_to_pg = HrpClickhouseToPostgresOperator(
            task_id="ch_to_pg",
            clickhouse_conn_id=CH_CONN, sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
            target_conn_id=PG_CONN, target_schema=PG_SCHEMA, target_table=T_CH_TO_PG,
            truncate=True,
        )
        v_ch_to_pg = validate_ch_to_pg()
        [setup_pg_t, setup_ch_t] >> ch_to_pg >> v_ch_to_pg

        pg_to_ch = HrpPostgresToClickhouseOperator(
            task_id="pg_to_ch",
            source_conn_id=PG_CONN, source_schema=PG_SCHEMA, source_table=SRC,
            target_conn_id=CH_CONN, target_schema=CH_SCHEMA, target_table=T_PG_TO_CH,
            target_truncate=True, target_fmt="CSV",
        )
        v_pg_to_ch = validate_ch_count.override(task_id="v_pg_to_ch")(T_PG_TO_CH)
        [setup_pg_t, setup_ch_t] >> pg_to_ch >> v_pg_to_ch

        pg_inc = HrpPostgresIncarnationInsertOperator(
            task_id="pg_incarnation_insert",
            conn_id=PG_CONN, select_query=f"SELECT * FROM {PG_SCHEMA}.{SRC}",
            target_schema=PG_SCHEMA, target_table=T_PG_INC,
        )
        v_pg_inc = validate_incarnation.override(task_id="v_pg_inc")(T_PG_INC)
        setup_pg_t >> pg_inc >> v_pg_inc

        ch_to_pg_inc = HrpClickhouseToPostgresIncarnationOperator(
            task_id="ch_to_pg_incarnation",
            clickhouse_conn_id=CH_CONN, sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
            target_conn_id=PG_CONN, target_schema=PG_SCHEMA, target_table=T_CH_TO_PG_INC,
        )
        v_ch_to_pg_inc = validate_incarnation.override(task_id="v_ch_to_pg_inc")(T_CH_TO_PG_INC)
        [setup_pg_t, setup_ch_t] >> ch_to_pg_inc >> v_ch_to_pg_inc

        # Источник и цель — одно подключение; heimdall_conn_id не нужен при clickhouse_source_conn_id
        ch_to_ch = HrpClickhouseToClickhouseOperator(
            task_id="ch_to_ch",
            source_sql=f"SELECT * FROM {CH_SCHEMA}.{SRC}",
            clickhouse_source_conn_id=CH_CONN, clickhouse_target_conn_id=CH_CONN,
            heimdall_conn_id=None, target_table=f"{CH_SCHEMA}.{T_CH_TO_CH}", truncate_target=True,
        )
        v_ch_to_ch = validate_ch_count.override(task_id="v_ch_to_ch")(T_CH_TO_CH)
        setup_ch_t >> ch_to_ch >> v_ch_to_ch

        # pg_to_pg — чисто PG; остальные три переливки кросс-системны (PG↔CH) → разные условия.
        make_gate(gate_cond("test_pg"), [pg_to_pg], "gate_pg_to_pg")
        make_gate(gate_cond("test_ch"), [ch_to_ch], "gate_ch_to_ch")
        make_gate(gate_cond("test_pg", "test_ch"), [ch_to_pg, pg_to_ch, ch_to_pg_inc], "gate_db_cross")
        # pg_incarnation в карантине: падает на баге insert_incarnation (sql.Literal вместо sql.SQL)
        # до пересборки пакета — держим за run_known_broken, чтобы не гасить остальной db_to_db.
        make_gate(gate_cond("run_known_broken", "test_pg"), [pg_inc], "gate_pg_incarnation")

    # ──────────────────────────── s3_utils ────────────────────────────────────
    # S3ToS3: перепаковка из несжатого источника во все целевые сжатия (post_file_check).
    pg_to_s3_none = next(o for o in exports if o.task_id == "pg_to_s3_none")
    ch_native_none = next(o for o in exports if o.task_id == "ch_native_to_s3_none")
    with TaskGroup(group_id="s3_utils", prefix_group_id=False):
        s3s3_ops = []
        for c in COMPRESSIONS_FULL:
            s3s3 = HrpS3ToS3Operator(
                task_id=f"s3_to_s3_{clabel(c)}",
                s3_bucket_source=BUCKET, s3_key_source=s3key("pg_to_s3", None),
                aws_conn_id_source=S3_CONN, compression_source=None,
                s3_bucket=BUCKET, s3_key=s3key("s3_to_s3", c),
                aws_conn_id=S3_CONN, compression=c, replace=True, post_file_check=True,
            )
            pg_to_s3_none >> s3s3
            s3s3_ops.append(s3s3)

        s3_archive = HrpS3ArchiveOperator(
            task_id="s3_archive",
            s3_keys_source=[s3key("pg_to_s3", None), s3key("ch_native", None)],
            s3_bucket_source=BUCKET, aws_conn_id_source=S3_CONN,
            s3_bucket=BUCKET, s3_key=f"{S3_PREFIX}archive/bundle.zip",
            aws_conn_id=S3_CONN, replace=True,
        )
        [pg_to_s3_none, ch_native_none] >> s3_archive

        # CheckS3FileHash: сверяем хэш gzip-выгрузки PostgresToS3 с тем, что оператор положил в XCom.
        check_hash = HrpCheckS3FileHash(
            task_id="check_s3_file_hash",
            s3_bucket=BUCKET, s3_key=s3key("pg_to_s3", "gzip"),
            aws_conn_id=S3_CONN, compression="gzip",
            checksum="{{ ti.xcom_pull(task_ids='pg_to_s3_gzip', key='checksum') }}",
        )
        pg_to_s3_gzip >> check_hash

        pg_ddl = HrpPostgresDDL(
            task_id="pg_ddl", table_name=SRC, schema=PG_SCHEMA, postgres_conn_id=PG_CONN,
        )
        setup_pg_t >> pg_ddl

        @task
        def validate_pg_ddl(**context):
            """Проверяет, что `pg_ddl` вернул DDL с `CREATE TABLE`."""
            ddl = context["ti"].xcom_pull(task_ids="pg_ddl")
            if not ddl or "CREATE TABLE" not in ddl:
                raise AirflowFailException(f"pg_ddl вернул некорректный DDL: {ddl!r}")
            logger.info("OK: pg_ddl вернул %d символов DDL", len(ddl))

        v_pg_ddl = validate_pg_ddl()
        pg_ddl >> v_pg_ddl

        # S3-утилиты гейтуются по test_s3, pg_ddl — по test_pg (чисто PG).
        # NB: s3_to_s3/check_hash физически читают выгрузки pg_to_s3_* — при test_pg=False
        # исходный файл не создастся и они уйдут в upstream_failed (☮️), что корректно.
        make_gate(gate_cond("test_s3"), [*s3s3_ops, s3_archive, check_hash], "gate_s3_utils")
        make_gate(gate_cond("test_pg"), [pg_ddl], "gate_pg_ddl")

    # ──────────────────────────── viewers ─────────────────────────────────────
    with TaskGroup(group_id="viewers", prefix_group_id=False):
        list_keys = HrpS3ListKeysOperator(
            task_id="s3_list_keys", bucket=BUCKET, prefix=S3_PREFIX, aws_conn_id=S3_CONN,
        )
        file_read = HrpS3FileReadOperator(
            task_id="s3_file_read",
            s3_bucket=BUCKET, s3_key=s3key("pg_to_s3", "gzip"),
            aws_conn_id=S3_CONN, compression="gzip", rows=10,
        )
        bucket_viewer = HrpS3BucketViewerOperator(task_id="s3_bucket_viewer", aws_conn_id=S3_CONN)

        @task
        def validate_list_keys(**context):
            """Проверяет, что `s3_list_keys` нашёл ключи тестового префикса."""
            keys = context["ti"].xcom_pull(task_ids="s3_list_keys") or []
            names = [k.get("Key") if isinstance(k, dict) else k for k in keys]
            if not any(str(n).startswith(S3_PREFIX) for n in names):
                raise AirflowFailException(f"s3_list_keys не вернул ключей с префиксом {S3_PREFIX}: {names}")
            logger.info("OK: s3_list_keys вернул %d ключей", len(names))

        v_list_keys = validate_list_keys()
        list_keys >> v_list_keys

        # viewers — S3-проверки (test_s3), но физически зависят от выгрузки pg_to_s3_gzip:
        # при test_pg=False файла не будет и file_read/list_keys уйдут в upstream_failed (☮️).
        make_gate(gate_cond("test_s3"), [list_keys, file_read, bucket_viewer],
                  "gate_viewers", upstream=[pg_to_s3_gzip, setup_pg_t])

    # ──────────────────────────── cluster ─────────────────────────────────────
    # ⚠ Требует system.clusters('datalab') и conn click-dlab-*. По умолчанию выключено (run_cluster).
    with TaskGroup(group_id="cluster", prefix_group_id=False):
        # DEV создаёт probe-таблицу; на прочих стендах она предсоздана — оператор
        # прогоняем на TRUNCATE (тоже cluster-DDL), не создавая объектов.
        cluster_sql = (
            f"CREATE TABLE IF NOT EXISTS {CH_SCHEMA}.hrp_cluster_probe (id Int32) ENGINE = MergeTree() ORDER BY id"
            if IS_DEV else
            f"TRUNCATE TABLE IF EXISTS {CH_SCHEMA}.hrp_cluster_probe"
        )
        cluster_op = HrpClickHouseClusterOperator(
            task_id="ch_cluster_ddl",
            sql=cluster_sql,
            clickhouse_conn_id=CH_CONN,
        )
        make_gate(gate_cond("run_cluster", "test_ch"), [cluster_op],
                  "gate_cluster", upstream=setup_ch_t)

    # ──────────────────────────── report ──────────────────────────────────────
    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        """Собирает статусы тасков прогона в заметку рана: упавшие первыми, успешные одной строкой."""
        # Дополнительно выводит строку статуса систем PG/CH/S3: отключена флагом,
        # недоступна (setup пропущен по ошибке соединения) или активна.
        dag_run = context["dag_run"]
        with create_session() as session:
            tis = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_run.dag_id, TaskInstance.run_id == dag_run.run_id)
                .order_by(TaskInstance.task_id)
                .all()
            )
            states = {ti.task_id: (ti.state or "no_status") for ti in tis}
            rows, ok, fail, skip = [], 0, 0, 0
            for ti in tis:
                # служебные таски не относятся к покрытию операторов: report/cleanup
                # и гейты-ветки (все с префиксом gate_) — их скип это норма
                if ti.task_id in ("report", "cleanup") or ti.task_id.startswith("gate_"):
                    continue
                state = ti.state or "no_status"
                if state == "success":
                    icon, ok = "✅", ok + 1
                elif state in ("skipped", "upstream_failed", "removed"):
                    icon, skip = "☮️", skip + 1
                    # on_skipped_callback ловит только явный AirflowSkipException; скипы по
                    # гейту/upstream он НЕ триггерит — до-записываем заметку здесь (ti в сессии,
                    # note закоммитит create_session). Заметки setup_* с причиной не перетираем.
                    if not ti.note:
                        ti.note = f"☮️ SKIPPED — {state} (отключено флагом системы или зависимостью)"
                else:
                    icon, fail = "❌", fail + 1
                rows.append(("❌☮️✅".index(icon[0]), f"{icon} `{ti.task_id}`" + (f" — {state}" if icon != "✅" else "")))
            session.commit()  # фиксируем до-записанные заметки скипнутых тасков

        # Статус систем: отключена флагом / недоступна (setup пропущен по ошибке) / активна.
        params = context["params"]
        sys_status = []
        for label, flag, setup_id in (("PG", "test_pg", "setup_pg"),
                                      ("CH", "test_ch", "setup_ch"),
                                      ("S3", "test_s3", "setup_s3")):
            st = states.get(setup_id, "no_status")
            if not params.get(flag):
                sys_status.append(f"{label} ⛔ отключена (`{flag}`=False)")
            elif st == "skipped":
                sys_status.append(f"{label} ⚠️ недоступна (setup пропущен)")
            elif st == "success":
                sys_status.append(f"{label} ✅ активна")
            else:
                sys_status.append(f"{label} ❌ ошибка setup ({st})")
        sys_status.append("кластер datalab " + ("✅ включён" if params.get("run_cluster") else "⛔ выключен (`run_cluster`)"))
        sys_line = "**Системы:** " + " · ".join(sys_status)

        headline = f"🧪 HRP operators: ✅ {ok} / ❌ {fail} / ☮️ {skip}"
        # Без таблицы: в 1000 символов заметки она вытесняла строки. Упавшие и пропущенные —
        # строкой на таск, успешные — одной строкой в конце: её и режет лимит
        lines = [row for rank, row in sorted(rows) if rank < 3]
        ok_ids = [row[2:] for rank, row in sorted(rows) if rank == 3]
        lines += [f"✅ ({len(ok_ids)}): " + ", ".join(ok_ids)] if ok_ids else []
        add_note(sys_line + "\n\n" + "\n".join(lines), context, level="DAG", title=headline)
        logger.info("%s | %s", headline, " · ".join(sys_status))

    # ──────────────────────────── cleanup ─────────────────────────────────────
    @task(task_id="cleanup", trigger_rule=TriggerRule.ALL_DONE)
    def cleanup(params=None):
        """Очищает PG/CH таблицы и S3-ключи (отрабатывает при любом исходе)."""
        # DEV — DROP таблиц/последовательностей (стенд владеет схемой);
        # прочие стенды — только TRUNCATE (таблицы предсозданы, не удаляем).
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        if not params.get("run_cleanup", True):
            logger.info("run_cleanup=False — пропускаем удаление (отладка: таблицы/ключи оставлены)")
            return
        pg = PostgresHook(postgres_conn_id=params["pg_conn_id"])
        pg_objects = [SRC, T_PG_TO_PG, T_CH_TO_PG]
        if IS_DEV:
            drops = [f"DROP TABLE IF EXISTS {PG_SCHEMA}.{t} CASCADE;" for t in pg_objects]
            for t in (T_PG_INC, T_CH_TO_PG_INC, T_S3_TO_PG):
                drops += [f"DROP TABLE IF EXISTS {PG_SCHEMA}.{t}_0 CASCADE;",
                          f"DROP TABLE IF EXISTS {PG_SCHEMA}.{t}_1 CASCADE;",
                          f"DROP SEQUENCE IF EXISTS {PG_SCHEMA}.{t}_inc_seq;"]
            pg.run("\n".join(drops))
        else:
            # Не-DEV: таблицы предсозданы — не удаляем, только очищаем (последовательности инкарнаций оставляем).
            truncs = [f"TRUNCATE TABLE {PG_SCHEMA}.{t};" for t in pg_objects]
            for t in (T_PG_INC, T_CH_TO_PG_INC, T_S3_TO_PG):
                truncs += [f"TRUNCATE TABLE {PG_SCHEMA}.{t}_0;",
                           f"TRUNCATE TABLE {PG_SCHEMA}.{t}_1;"]
            pg.run("\n".join(truncs))

        ch = ClickHouseHook(clickhouse_conn_id=params["ch_conn_id"])
        for t in (SRC, T_PG_TO_CH, T_S3_TO_CH, T_S3_TO_CH_LIST, T_S3_TO_CH_TR, T_CH_TO_CH, "hrp_cluster_probe"):
            if IS_DEV:
                ch.execute(f"DROP TABLE IF EXISTS {CH_SCHEMA}.{t}")
            else:
                ch.execute(f"TRUNCATE TABLE IF EXISTS {CH_SCHEMA}.{t}")

        s3 = S3Hook(aws_conn_id=params["s3_conn_id"])
        _s3_purge_prefix(s3, params["s3_bucket"])
        logger.info("Cleanup complete")

    # Все рабочие таски → report → cleanup (оба ALL_DONE). Включаем КАЖДЫЙ таск как прямой
    # upstream report, чтобы ALL_DONE дождался всех валидаций (Airflow дедуплицирует рёбра).
    report_t = report()
    cleanup_t = cleanup()
    all_tasks = [
        setup_pg_t, setup_ch_t, setup_s3_t,
        *exports, *s3s3_ops,
        v_pg_sanitize, v_ch_sanitize,
        s3_to_ch_csv, s3_to_ch_tsv, v_s3_csv, v_s3_tsv,
        s3_to_pg, v_s3_to_pg, s3_to_ch_tr, v_s3_to_ch_tr,
        pg_to_pg, v_pg_to_pg, ch_to_pg, v_ch_to_pg, pg_to_ch, v_pg_to_ch,
        pg_inc, v_pg_inc, ch_to_pg_inc, v_ch_to_pg_inc, ch_to_ch, v_ch_to_ch,
        s3_archive, check_hash, pg_ddl, v_pg_ddl,
        list_keys, v_list_keys, file_read, bucket_viewer,
        cluster_op,
    ]
    # ВАЖНО: гейты (@task.branch) НЕ должны быть прямым upstream report — branch
    # принудительно скипает все прямые downstream вне своего списка, перебивая
    # ALL_DONE. Их подопечные ops и так в all_tasks, поэтому report всё дожидается.
    all_tasks >> report_t >> cleanup_t


test_hrp_operators_dag()
