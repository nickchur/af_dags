"""### 🧪 DAG: Регрессионный стенд операторов HRP
*2026-10-23 09:49 MSK · v2.7 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Регресс операторов `hrp_operators` на каждый релиз. Каждый оператор — своя группа задач внутри
группы `tests`, каждый тест — одна задача: сам готовит вход (таблицу, файл в S3), выполняет оператор
как в бою (Jinja окружением дага, политика контура) и сверяет результат, после себя убирает. Тесты
группы идут друг за другом, группы — параллельно. Оператора нет в пакете или он запрещён политикой —
группы нет; сломан импорт — в группе одна красная задача. Первая задача `systems` проверяет системы
и права, последняя `report` пишет сводку по группам. Запуск вручную (`@once`).
Стандартные операторы Airflow и провайдеров из замера сигмы — в главной группе `test_standart`,
по тем же правилам.

PG: `conn.схема` — таблицы в схеме (нужны CREATE и DROP), только `conn` — временные таблицы
сессии, в базе после рана ничего не остаётся. GP — так же, как PG.

Вердикт (гейт релиза в DPM): ран `failed`, если есть ❌ или включённая система недоступна;
детали — XCom `verdict` таска `report`.

Подробно: [tools/readme.md — test_hrp_operators](../../_plugin_dag_docs/?doc=tools/readme.md#test_hrp_operatorspytest_hrp_operatorspy)
"""

import csv
import datetime as dt
import gzip
import hashlib
import importlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import zipfile
from contextlib import closing, contextmanager, nullcontext
from dataclasses import dataclass, field
from decimal import Decimal
from logging import getLogger
from unittest import mock

# Пути для локальной разработки. В продакшне пакет устанавливается через pip,
# поэтому переменные не задаются и sys.path не изменяется.
for _env_var in ("HRP_ETL_CORE_PATH", "HRP_OPERATORS_SRC_PATH"):
    _path = os.environ.get(_env_var)
    if _path:
        sys.path.insert(0, _path)

from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: E402
from airflow.sdk import Param, TaskGroup, TriggerRule, dag, task  # noqa: E402
from airflow.sdk.exceptions import AirflowFailException, AirflowSensorTimeout, AirflowSkipException  # noqa: E402

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, log_bucket_path, on_callback, saved_params, store_params,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, log_bucket_path, on_callback, saved_params, store_params,
    )

logger = getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# ───────────────────────────────── Настройки ──────────────────────────────────
# Форма запуска сохраняется галочкой save_params и на разборе становится значениями по умолчанию.
# Подключения, схемы и бакет тесты берут из формы рана, а не из разбора: смена действует сразу
PARAMS_VAR = "tools_test_hrp_operators_params"
SAVED = saved_params(PARAMS_VAR)

# PG — «conn» или «conn.схема», CH — «conn.база», S3 — «conn://бакет/папка». Папка в S3 по умолчанию —
# test-operators в бакете логов; каждый тест пишет и чистит только свою подпапку группа/тест
# схема и база — строчными: имена в SQL идут без кавычек, «HrpTest» база приведёт к hrptest
PG_PATTERN = r"^[^\s.]+(\.[a-z_][a-z0-9_]*)?$"
DB_PATTERN = r"^[^\s.]+\.[a-z_][a-z0-9_]*$"
S3_PATTERN = r"^[^\s:/]+://[a-z0-9][a-z0-9.-]+/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*/?$"
PKG = "sber_app_dataplatform_etl_core.hrp_operators"
PROBE = "rg_probe"


# ───────────────────────── Единый источник данных ─────────────────────────────
# (name, pg_type, ch_type_typed, ch_type_string, comment) — комменты обязательны для QG.
COLUMNS = [
    ("id",          "integer",          "Int32",                    "String", "Идентификатор строки"),
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
EXPECTED_ROWS = len(ROWS)


def _pg_create_sql(table: str, temp: bool, incarnation: bool = False, inc_col: bool = True) -> list[str]:
    """CREATE (TEMP) TABLE источника/цели; incarnation — _0/_1 + последовательность _inc_seq.

    table — с схемой в режиме схемы, без схемы во временном. Комментарии — только у физических
    таблиц (Quality Gate). DROP здесь нет: остатки убирает подготовка теста по префиксу.
    """
    t = "TEMP " if temp else ""
    # без PRIMARY KEY: в GP таблицы по умолчанию append-only, уникальный индекс на них запрещён
    cols = (["incarnation integer"] if incarnation and inc_col else []) + [
        f"{name} {pg_type}" for name, pg_type, *_ in COLUMNS]
    tables = [f"{table}_0", f"{table}_1"] if incarnation else [table]
    stmts = [f"CREATE {t}SEQUENCE {table}_inc_seq START WITH 0 MINVALUE 0"] if incarnation else []
    for tbl in tables:
        stmts.append(f"CREATE {t}TABLE {tbl} ({', '.join(cols)})")
        if not temp:
            stmts.append(f"COMMENT ON TABLE {tbl} IS 'HRP regression'")
            stmts += [f"COMMENT ON COLUMN {tbl}.{name} IS '{comment}'" for name, *_, comment in COLUMNS]
    return stmts


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


def _ch_create_sql(table: str, typed: bool, lead: str = "") -> str:
    """CREATE TABLE для ClickHouse: typed — типы источника (Array/Nullable), иначе все String.

    lead — колонка перед колонками источника (S3ToClickhouseTransformed пишет file_name первой).
    """
    idx = 2 if typed else 3  # позиция типа в COLUMNS
    cols = ", ".join([lead] * bool(lead) + [f"{c[0]} {c[idx]}" for c in COLUMNS])
    return f"CREATE TABLE {table} ({cols}) ENGINE = MergeTree() ORDER BY {'id' if typed else 'tuple()'}"


# xstream_sanitize: ; таб и перевод строки вырезаются, кавычки и запятые остаются
SANITIZED_SPECIAL = 'Спец: "кавычки", запятая,  точка-с-запятой,таб,перенос,  бэкслеш, №42'
# формат HrpPostgresToS3ListOperator по умолчанию: TSV без кавычек, экранирование обратным слешем
TSV = {"delimiter": "\t", "quoting": csv.QUOTE_NONE, "escapechar": "\\"}


def _unpack(raw: bytes, compression) -> bytes:
    """Содержимое файла выгрузки: один файл внутри архива."""
    if compression == "gzip":
        return gzip.decompress(raw)
    if compression == "zip":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            return _single(z.namelist(), z.read)
    if compression == "tar.gz":
        # tar.gz ядра — заголовок tar с размером 0 и данные за ним сырыми (размер потока заранее не
        # известен): tarfile и tar xzf видят пустой файл, читает только TarGzStreamReader ядра
        from sber_app_dataplatform_etl_core.hrp_operators.utils.compress_utils import UNCOMPRESSOR_MAP
        with UNCOMPRESSOR_MAP["tar.gz"](io.BytesIO(raw)) as r:
            return b"".join(iter(lambda: r.read(1 << 16), b"")).rstrip(b"\n\0") + b"\n"
    return raw


def _single(names, read) -> bytes:
    if len(names) != 1:
        raise AirflowFailException(f"в архиве {len(names)} файлов, ждём один: {names}")
    return read(names[0])


def _csv_bytes(rows=ROWS, header=True) -> bytes:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows([COL_NAMES, *rows] if header else rows)
    return buf.getvalue().encode()


def _tsv_bytes(rows=ROWS, esc=lambda s: s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")) -> bytes:
    """TabSeparatedWithNames по правилам ClickHouse: \\t, \\n и \\\\ экранами; NULL — пусто."""
    lines = [COL_NAMES] + [["" if v is None else esc(str(v)) for v in r] for r in rows]
    return "".join("\t".join(x) + "\n" for x in lines).encode()


def _check_sanitized(row: dict):
    """Строка 2 после xstream_sanitize и sanitize_array: спецсимволы вырезаны и в строке, и в массиве."""
    errors = []
    if row["s_special"] != SANITIZED_SPECIAL:
        errors.append(f"s_special={row['s_special']!r}, ждём {SANITIZED_SPECIAL!r}")
    if "x;y" in row["arr_text"] or "xy" not in row["arr_text"]:
        errors.append(f"arr_text не очищен (sanitize_array): {row['arr_text']!r}")
    if errors:
        raise AirflowFailException("; ".join(errors))


# ─────────────────────────────── Реестр групп ─────────────────────────────────
@dataclass
class Op:
    """Группа регресса: оператор, системы, которые ему нужны, и его тесты (функции case(t))."""
    group: str
    module: str
    cls: str
    systems: set
    cases: list
    pkg: str = PKG
    policy: bool = True  # False — утилита пакета (lineage), не оператор: allowed_operators её не касается
    loaded: type | None = field(default=None, repr=False)


def _policy():
    """Модуль политики контура, если он есть; без него разрешено всё."""
    if "airflow_local_settings" in sys.modules:
        return sys.modules["airflow_local_settings"]
    try:
        return importlib.import_module("airflow_local_settings")
    except ModuleNotFoundError as e:
        if e.name != "airflow_local_settings":
            raise  # политика есть, но сломана: молча разрешить всё нельзя
        return None


def _load(op: Op) -> tuple[type | None, str, str]:
    """(класс, статус, причина): ok | missing | forbidden | broken."""
    mod = f"{op.pkg}.{op.module}"
    try:
        cls = getattr(importlib.import_module(mod), op.cls, None)
    except ModuleNotFoundError as e:
        # нет самого модуля (или его пакета) — оператора нет; нет зависимости модуля — он сломан
        if e.name and (mod == e.name or mod.startswith(e.name + ".")):
            return None, "missing", f"нет модуля {mod}"
        return None, "broken", f"{type(e).__name__}: {e}"
    except Exception as e:  # SyntaxError, ImportError имени изнутри модуля и т. п.
        return None, "broken", f"{type(e).__name__}: {e}"
    if cls is None:
        return None, "missing", f"нет {op.cls} в {mod}"
    allowed = getattr(_policy(), "ALLOWED_OPERATORS", None)
    if allowed is not None and op.policy and op.cls not in allowed:
        return None, "forbidden", "нет в allowed_operators"
    return cls, "ok", ""


# ───────────────────────────── Сессии temp-режима ─────────────────────────────
_REAL_GET_CONN = PostgresHook.get_conn


class _Shared:
    """Сессия временных таблиц: оператор её «закрывает», а таблицы живы до конца теста.

    close() ведёт себя как закрытие для данных: незавершённая транзакция откатывается.
    """

    def __init__(self, real):
        object.__setattr__(self, "_r", real)

    def __getattr__(self, n):
        return getattr(self._r, n)

    def __setattr__(self, n, v):
        setattr(self._r, n, v)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        # как `with conn:` у psycopg: без ошибки — фиксация, с ошибкой — откат; соединение живёт дальше
        if exc_type is None:
            self._r.commit()
        else:
            self._r.rollback()

    def close(self):
        if self._r.info.transaction_status != 0:  # не IDLE: открытая или упавшая транзакция
            self._r.rollback()


# ──────────────────────────────── Рантайм теста ───────────────────────────────
class T:
    """Контекст одного теста: подключения из формы рана, имена с префиксом теста, уборка."""

    def __init__(self, context, op: Op, case: str):
        self.context, self.op, self.cls = context, op, op.loaded
        p = context["params"]
        self.prefix = f"rg_{op.group}_{case}"[:40]
        self.pg_conn, _, schema = p["pg"].partition(".")
        self.temp = not schema
        self.pg_schema = schema or "pg_temp"
        self.ch_conn, _, self.ch_db = p["ch"].partition(".")
        self.s3_conn, _, rest = p["s3"].partition("://")
        self.bucket, _, folder = rest.partition("/")
        self.s3_prefix = f"{folder.strip('/')}/{op.group}/{case}/"
        self._sessions: dict = {}
        self._main = None
        self._s3 = None
        self._ch = None
        self.systems: dict = {}
        self._ops = 0
        self._tmp = tempfile.TemporaryDirectory(prefix=self.prefix, ignore_cleanup_errors=True)
        self.tmp = self._tmp.name
        self.not_cleaned: list[str] = []

    # ── решение «идти или ☮️» ──
    def gate(self):
        p = self.context["params"]
        if not p.get(f"op_{self.op.group}", True):
            raise AirflowSkipException(f"{self.op.group}: выключен (op_{self.op.group}=False)")
        self.systems = systems = self.context["ti"].xcom_pull(task_ids="systems")
        if systems is None:  # {} — все системы выключены, это не сбой
            raise AirflowFailException("нет результата задачи systems")
        # порядок ch < ch_http < cluster: без CH про его HTTP и кластер не спрашиваем
        for s in sorted(self.op.systems):
            flag = {"pg": "test_pg", "ch": "test_ch", "s3": "test_s3", "ch_http": "ch_http"}.get(s)
            if flag and not p.get(flag):
                raise AirflowSkipException(f"{flag}=False")
            st = (systems.get("ch") or {}).get("cluster") or {} if s == "cluster" else systems.get(s)
            if s == "cluster" and not st.get("ok"):
                raise AirflowSkipException(f"кластер: {st.get('reason') or 'нет сведений'}")
            if st is not None and not st["ok"]:
                raise AirflowSkipException(f"{s.upper()} недоступен: {st['reason']}")
        pg = systems.get("pg") or {}
        if "pg" in self.op.systems and not self.temp and not pg.get("ddl"):
            raise AirflowFailException(pg.get("reason") or f"нет DROP (CREATE) на {self.context['params']['pg']}")

    # ── PG ──
    def session(self, name="main"):
        """Сырое соединение PG: в temp-режиме — отдельная сессия на имя, иначе одно на тест."""
        name = name if self.temp else "main"
        if name not in self._sessions:
            conn = _REAL_GET_CONN(PostgresHook(postgres_conn_id=self.pg_conn))
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 60000")
            conn.commit()
            self._sessions[name] = conn
        return self._sessions[name]

    def pg(self, sqls, params=None, session="main", fetch=False):
        conn = self.session(session)
        try:
            with conn.cursor() as cur:
                rows = None
                for s in [sqls] if isinstance(sqls, str) else sqls:
                    cur.execute(s, params)
                    rows = cur.fetchall() if fetch else None
            conn.commit()
            return rows
        except BaseException:
            conn.rollback()
            raise

    def name(self, x: str) -> str:
        return f"{self.prefix}_{x}"

    def full(self, table: str) -> str:
        return table if self.temp else f"{self.pg_schema}.{table}"

    def pg_table(self, x: str, rows=(), session="main", **kw) -> str:
        """Создаёт таблицу теста (и наполняет rows); возвращает имя без схемы."""
        table = self.name(x)
        self.pg(_pg_create_sql(self.full(table), self.temp, **kw), session=session)
        if rows:
            conn = self.session(session)
            with conn.cursor() as cur:
                cur.executemany(f"INSERT INTO {self.full(table)} ({', '.join(COL_NAMES)}) "
                                f"VALUES ({', '.join(['%s'] * len(COL_NAMES))})", rows)
            conn.commit()
        return table

    def pg_rows(self, table: str, session="main") -> list:
        return self.pg(f"SELECT * FROM {self.full(table)} ORDER BY id", session=session, fetch=True)

    def pg_copy_out(self, query: str, header=False, session="main") -> bytes:
        """CSV средствами самой базы: вход для S3 → PG в том виде, в каком его пишет PG (массивы, NULL)."""
        conn = self.session(session)
        with conn.cursor() as cur, cur.copy(f"COPY ({query}) TO STDOUT WITH CSV{' HEADER' if header else ''}") as cp:
            data = b"".join(bytes(b) for b in cp)
        conn.commit()
        return data

    def pg_active(self, table: str, session="main") -> tuple[int, list]:
        """(активная инкарнация, её строки без колонки incarnation)."""
        (active,), = self.pg(f"SELECT last_value FROM {self.full(table)}_inc_seq", session=session, fetch=True)
        rows = self.pg(f"SELECT {', '.join(COL_NAMES)} FROM {self.full(table)}_{active} ORDER BY id",
                       session=session, fetch=True)
        return active, rows

    @contextmanager
    def pg_sessions(self, order):
        """temp-режим: get_conn оператора отдаёт сессии в порядке order (в каком их откроет оператор)."""
        if not self.temp or not order:
            yield
            return
        it = iter(order)

        def _get_conn(_hook):
            try:
                return _Shared(self.session(next(it)))
            except StopIteration:
                raise AssertionError(f"оператор открыл больше соединений, чем {list(order)}") from None

        with mock.patch.object(PostgresHook, "get_conn", _get_conn):
            yield

    # ── CH ──
    def ch(self, sql: str):
        if self._ch is None:
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            self._ch = ClickHouseHook(clickhouse_conn_id=self.ch_conn)
        return self._ch.execute(sql)

    def ch_full(self, table: str) -> str:
        return f"{self.ch_db}.{table}"

    def ch_table(self, x: str, typed=True, rows=(), lead="") -> str:
        """Создаёт таблицу CH теста (и наполняет rows); возвращает полное имя."""
        table = self.ch_full(self.name(x))
        self.ch(_ch_create_sql(table, typed, lead))
        if rows:
            self.ch(f"INSERT INTO {table} VALUES " + ", ".join("(" + ", ".join(map(_ch_lit, r)) + ")" for r in rows))
        return table

    def pushed(self, key: str):
        """XCom, который оператор положил в задачу теста (row_count и т. п.)."""
        ti = self.context["ti"]
        return ti.xcom_pull(task_ids=ti.task_id, key=key)

    # ── S3 ──
    @property
    def s3(self):
        if self._s3 is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            self._s3 = S3Hook(aws_conn_id=self.s3_conn)
        return self._s3

    def s3_put(self, name: str, data: bytes) -> str:
        key = self.s3_prefix + name
        self.s3.load_bytes(data, key=key, bucket_name=self.bucket, replace=True)
        return key

    def s3_get(self, key: str) -> bytes:
        return self.s3.get_key(key, self.bucket).get()["Body"].read()

    def s3_keys(self, name: str) -> list[str]:
        return sorted(self.s3.list_keys(bucket_name=self.bucket, prefix=self.s3_prefix + name) or [])

    # ── оператор ──
    def run(self, sessions=(), spy=None, **kw):
        """Создаёт оператор, рендерит шаблоны окружением дага, применяет политику, выполняет.

        sessions — порядок сессий для temp-режима; spy — метод оператора, вызовы которого считаем.
        Возвращает (результат execute, mock шпиона или None).
        """
        ctx, dag_ = self.context, self.context["dag"]
        # dag= нужен политике: тег pg_export она читает у дага задачи
        self._ops += 1  # второй run() в одном случае не должен дать DuplicateTaskIdFound
        op = self.cls(task_id=f"{ctx['ti'].task_id.split('.')[-1]}__op{self._ops}", dag=dag_, **kw)
        op.render_template_fields(ctx, jinja_env=dag_.get_template_env())
        policy = _policy()
        if policy is not None and hasattr(policy, "task_policy"):
            policy.task_policy(op)  # нарушение политики — ❌ теста, как падение разбора в бою
        # как исполнитель задачи: копия с замком, иначе присвоение template-поля в execute (op_kwargs у
        # PythonOperator) ищет в нём XComArg и уходит в рекурсию по контексту
        op = op.prepare_for_execution()
        spy_cm = mock.patch.object(op, spy, wraps=getattr(op, spy)) if spy else nullcontext()
        with self.pg_sessions(sessions), spy_cm as m:
            result = op.execute(ctx)
        return result, m

    def expect_rows(self, result, actual: int, required=False):
        """REQ-19: в цели EXPECTED_ROWS строк, а row_count оператора (если есть, required — обязан быть)
        равен числу в цели."""
        rc = result.get("row_count") if isinstance(result, dict) else None
        if actual != EXPECTED_ROWS or (rc is None and required) or (rc is not None and int(rc) != actual):
            raise AirflowFailException(f"в цели {actual} строк (ждём {EXPECTED_ROWS}), row_count={rc}")

    # ── уборка ──
    def _drop_own(self, only=None):
        """Удаляет объекты теста: таблицы и последовательности по префиксу, ключи под своей папкой S3.

        only — одна система (pg, ch, s3); без него — все."""
        if "pg" in self.op.systems and not self.temp and only in (None, "pg"):
            main = self._sessions.get("main")
            if main is not None and main.info.transaction_status != 0:
                main.rollback()  # таймаут посреди запроса: иначе «current transaction is aborted»
            like = self.prefix.replace("_", r"\_") + r"\_%"
            rels = self.pg("SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                           "WHERE n.nspname = %s AND c.relname LIKE %s AND c.relkind IN ('r', 'p', 'S', 'v')",
                           (self.pg_schema, like), fetch=True)
            # таблицы раньше последовательностей: на последовательность ссылается default колонки
            for rel, kind in sorted(rels, key=lambda r: r[1] == "S"):
                what = {"S": "SEQUENCE", "v": "VIEW"}.get(kind, "TABLE")
                self.pg(f"DROP {what} IF EXISTS {self.pg_schema}.{rel} CASCADE")
        if "ch" in self.op.systems and only in (None, "ch"):
            for (rel,) in self.ch(f"SELECT name FROM system.tables WHERE database = '{self.ch_db}' "
                                  f"AND startsWith(name, '{self.prefix}_')"):
                self.ch(f"DROP TABLE IF EXISTS {self.ch_full(rel)}")
        if "s3" in self.op.systems and only in (None, "s3"):
            keys = self.s3.list_keys(bucket_name=self.bucket, prefix=self.s3_prefix)
            if keys:
                # у S3Hook delete_objects принимает bucket, а не bucket_name
                self.s3.delete_objects(bucket=self.bucket, keys=keys)

    def clean_leftovers(self):
        """Остатки убитого прогона: DROP … IF EXISTS по префиксу и очистка своей папки до подготовки."""
        self._drop_own()

    def cleanup(self):
        # каждая система — отдельно: упавшая уборка PG не должна оставить таблицы CH и ключи S3
        for system in ("pg", "ch", "s3"):
            try:
                self._drop_own(only=system)
            except Exception as e:  # исход теста уборка не подменяет (REQ-23)
                logger.exception("уборка %s (%s) не удалась", self.prefix, system)
                self.not_cleaned.append(f"{self.prefix} {system}: {type(e).__name__}: {e}"[:300])
                add_note(self.not_cleaned[-1], self.context, level="task", title="⚠️ не убрано")

    def close(self):
        for conn in self._sessions.values():
            try:
                conn.close()
            except Exception:
                logger.warning("сессия PG %s не закрылась", self.prefix, exc_info=True)
        self._sessions.clear()
        self._tmp.cleanup()


# ─────────────────────────────── Тесты: PG → PG ───────────────────────────────
def _pg_to_pg(t: T, **kw):
    src = t.pg_table("src", ROWS, session="src")
    tgt = t.pg_table("tgt", session="tgt")
    # temp: оператор открывает источник, цель, затем цель ещё раз — для сверки числа строк
    result, _ = t.run(sessions=("src", "tgt", "tgt"),
                      source_conn_id=t.pg_conn, source_schema=t.pg_schema, source_table=src,
                      target_conn_id=t.pg_conn, target_schema=t.pg_schema, target_table=tgt,
                      truncate=True, post_count_check=True, **kw)
    got, want = t.pg_rows(tgt, "tgt"), t.pg_rows(src, "src")
    if got != want:
        raise AirflowFailException(f"цель отличается от источника: {got!r} ≠ {want!r}"[:1000])
    t.expect_rows(result, len(got))


def pg_to_pg_full(t: T):
    """PG → PG: перелив таблицы с truncate и сверкой числа строк, цель = источник построчно"""
    _pg_to_pg(t)


def pg_to_pg_chunked(t: T):
    """PG → PG порциями: chunk_column=id, chunk_size=2, цель = источник построчно"""
    _pg_to_pg(t, chunk_column="id", chunk_size=2)


# ─────────────────────────────── Тесты: S3 → S3 ───────────────────────────────
def _s3_to_s3(t: T, compression, post_file_check, ext):
    src = t.s3_put("src.csv", _csv_bytes())
    tgt = f"{t.s3_prefix}tgt.csv{ext}"
    _, spy = t.run(spy="verify_file_hash",
                   s3_bucket_source=t.bucket, s3_key_source=src, aws_conn_id_source=t.s3_conn,
                   s3_bucket=t.bucket, s3_key=tgt, aws_conn_id=t.s3_conn,
                   compression=compression, post_file_check=post_file_check, replace=True)
    return t.s3_get(tgt), _csv_bytes(), spy


def s3_to_s3_gzip(t: T):
    """S3 → S3, gzip: перепаковка несжатого файла, оператор сверяет хэш"""
    raw, want, spy = _s3_to_s3(t, "gzip", True, ".gz")
    if gzip.decompress(raw) != want or spy.call_count != 1:
        raise AirflowFailException(f"gzip не совпал с источником или хэш не сверен (сверок {spy.call_count})")


def s3_to_s3_zip(t: T):
    """S3 → S3, zip: в архиве один файл, равный источнику"""
    raw, want, _ = _s3_to_s3(t, "zip", True, ".zip")
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names = z.namelist()
        if len(names) != 1 or z.read(names[0]) != want:
            raise AirflowFailException(f"zip: {names}, содержимое не совпало с источником")


def s3_to_s3_tpl(t: T):
    """S3 → S3 шаблоном: compression и post_file_check='False' из {{ }} — gzip без сверки хэша"""
    raw, want, spy = _s3_to_s3(t, "{{ 'gzip' }}", "{{ 'False' }}", ".gz")
    if gzip.decompress(raw) != want:
        raise AirflowFailException("gzip из шаблона не совпал с источником")
    if spy.call_count:
        raise AirflowFailException("post_file_check='False' из шаблона не подействовал: хэш сверялся")


# ──────────────────────────── Тесты: PG → PG в инкарнацию ─────────────────────
def _pg_inc(t: T, tpl=False, **kw):
    src = t.pg_table("src", ROWS)
    tgt = t.name("tgt")
    t.pg(_pg_create_sql(t.full(tgt), t.temp, incarnation=True))
    query, schema = f"SELECT {', '.join(COL_NAMES)} FROM {t.full(src)}", t.pg_schema
    if tpl:
        query, tgt_arg, schema = f"{{{{ {query!r} }}}}", f"{{{{ {tgt!r} }}}}", f"{{{{ {schema!r} }}}}"
    result, _ = t.run(sessions=("main",), conn_id=t.pg_conn, select_query=query,
                      target_table=tgt_arg if tpl else tgt, target_schema=schema, **kw)
    active, got = t.pg_active(tgt)
    if active != 1 or got != t.pg_rows(src):
        raise AirflowFailException(f"активна инкарнация {active} (ждём 1), строки {'не ' if got != t.pg_rows(src) else ''}"
                                   f"совпали с источником: {got!r}"[:1000])
    t.expect_rows(result, len(got), required=True)


def pg_inc_full(t: T):
    """PG → PG в инкарнацию: вставка запросом в неактивную копию _1, переключение, row_count = 3"""
    _pg_inc(t)


def pg_inc_chunked(t: T):
    """PG → PG в инкарнацию порциями: chunk_column=id, chunk_size=2"""
    _pg_inc(t, chunk_column="id", chunk_size=2)


def pg_inc_tpl(t: T):
    """PG → PG в инкарнацию шаблоном: запрос, схема, таблица и chunk_column из {{ }}"""
    _pg_inc(t, tpl=True, chunk_column="{{ 'id' }}", chunk_size=2)


# ─────────────────────────────── Тесты: PG DDL ────────────────────────────────
def _pg_ddl(t: T, tpl=False):
    if t.temp:  # DDL читает каталог своим соединением SQLAlchemy — временную таблицу чужой сессии не видно
        raise AirflowSkipException("нужна схема в pg: временная таблица из другой сессии не видна")
    src = t.pg_table("src")
    name, schema = (f"{{{{ {src!r} }}}}", f"{{{{ {t.pg_schema!r} }}}}") if tpl else (src, t.pg_schema)
    ddl, _ = t.run(table_name=name, schema=schema, postgres_conn_id=t.pg_conn)
    missing = [c for c in COL_NAMES if c not in (ddl or "")]
    if not ddl or "CREATE TABLE" not in ddl or src not in ddl or missing:
        raise AirflowFailException(f"DDL без CREATE TABLE {src} или колонок {missing}: {ddl!r}"[:1000])


def pg_ddl_table(t: T):
    """PG DDL: CREATE TABLE таблицы теста со всеми колонками"""
    _pg_ddl(t)


def pg_ddl_tpl(t: T):
    """PG DDL шаблоном: table_name и schema из {{ }}"""
    _pg_ddl(t, tpl=True)


# ─────────────────────────────── Тесты: PG → S3 ───────────────────────────────
def _pg_to_s3(t: T, compression, tpl=False):
    src = t.pg_table("src", ROWS)
    key = f"{t.s3_prefix}out.csv"
    kw = dict(table_name=src, schema=t.pg_schema, s3_key=key)
    if tpl:
        kw = {k: f"{{{{ {v!r} }}}}" for k, v in kw.items()}
    _, spy = t.run(sessions=("main",), spy="verify_file_hash", s3_bucket=t.bucket, postgres_conn_id=t.pg_conn,
                   aws_conn_id=t.s3_conn, compression=compression, replace=True, post_file_check=True, **kw)
    keys = t.s3_keys("out")
    # порядок строк не сверяем: на GP SELECT * без ORDER BY отдаёт их в порядке сегментов
    want = sorted(csv.reader(io.StringIO(t.pg_copy_out(f"SELECT * FROM {t.full(src)}").decode())))
    got = sorted(csv.reader(io.StringIO(_unpack(t.s3_get(keys[0]), compression).decode()))) if len(keys) == 1 else None
    if got != want or spy.call_count != 1:
        raise AirflowFailException(f"файл {keys} не равен CSV таблицы или хэш не сверен (сверок {spy.call_count})")


def pg_to_s3_gzip(t: T):
    """PG → S3, gzip: файл .csv.gz равен COPY … CSV таблицы, оператор сверяет хэш"""
    _pg_to_s3(t, "gzip")


def pg_to_s3_none(t: T):
    """PG → S3 без сжатия: файл равен COPY … CSV таблицы"""
    _pg_to_s3(t, None)


def pg_to_s3_tpl(t: T):
    """PG → S3 шаблоном: таблица, схема и ключ из {{ }}"""
    _pg_to_s3(t, "gzip", tpl=True)


# ─────────────────────────── Тесты: PG → S3 частями ───────────────────────────
def _pg_to_s3_list(t: T, compression, fmt=TSV, with_header=True, **kw) -> dict[str, list[str]]:
    """Выгружает таблицу теста, сверяет файл; возвращает строки файла по id."""
    src = t.pg_table("src", ROWS)
    ext = {"gzip": ".gz", "zip": ".zip", "tar.gz": ".tar.gz"}.get(compression, "")
    _, spy = t.run(sessions=("main",), spy="verify_file_hash", table_name=src, schema=t.pg_schema,
                   s3_bucket=t.bucket, s3_key=f"{t.s3_prefix}out.csv{ext}", postgres_conn_id=t.pg_conn,
                   aws_conn_id=t.s3_conn, compression=compression, replace=True, post_file_check=True, **kw)
    keys = t.s3_keys("out")
    if len(keys) != 1 or not keys[0].endswith(f"out_1.csv{ext}") or spy.call_count != 1:
        raise AirflowFailException(f"ждём один файл out_1.csv{ext} со сверкой хэша: {keys}, сверок {spy.call_count}")
    rows = list(csv.reader(io.StringIO(_unpack(t.s3_get(keys[0]), compression).decode()), **fmt))
    if with_header and rows[:1] != [COL_NAMES]:
        raise AirflowFailException(f"заголовок {rows[:1]!r}, ждём {COL_NAMES}")
    data = {r[0]: r for r in (rows[1:] if with_header else rows)}  # на GP порядок строк — сегментов
    if sorted(data) != sorted(str(r[0]) for r in ROWS) or len(data) != len(rows) - with_header:
        raise AirflowFailException(f"id строк файла: {[r[:1] for r in rows]}, ждём 1, 2, 3 по разу")
    # значения не сверяем (формат TSV оператора ≠ COPY), но сдвиг разделителя ловит число полей
    if bad := [r[:1] for r in data.values() if len(r) != len(COL_NAMES)]:
        raise AirflowFailException(f"строки {bad}: полей не {len(COL_NAMES)}")
    return data


def pg_to_s3_list_gzip(t: T):
    """PG → S3 частями, gzip: TSV с заголовком, три строки, оператор сверяет хэш"""
    _pg_to_s3_list(t, "gzip")


def pg_to_s3_list_zip(t: T):
    """PG → S3 частями, zip: в архиве один TSV с заголовком"""
    _pg_to_s3_list(t, "zip")


def pg_to_s3_list_targz(t: T):
    """PG → S3 частями, tar.gz: в архиве один TSV с заголовком"""
    _pg_to_s3_list(t, "tar.gz")


def pg_to_s3_list_none(t: T):
    """PG → S3 частями без сжатия: TSV с заголовком"""
    _pg_to_s3_list(t, None)


def pg_to_s3_list_sanitize(t: T):
    """PG → S3 частями с очисткой: xstream_sanitize и sanitize_array вырезают спецсимволы строки 2"""
    _check_sanitized(dict(zip(COL_NAMES, _pg_to_s3_list(t, "gzip", xstream_sanitize=True, sanitize_array=True)["2"])))


def pg_to_s3_list_tpl(t: T):
    """PG → S3 частями шаблоном: header='false' и format_params JSON-строкой из {{ }} — CSV без заголовка"""
    fmt = {"delimiter": ",", "quoting": csv.QUOTE_MINIMAL}
    _pg_to_s3_list(t, "gzip", fmt={"delimiter": ","}, with_header=False,
                   **{"header": "{{ 'false' }}", "format_params": "{{ '%s' }}" % json.dumps(fmt)})


# ─────────────────────────────── Тесты: S3 → PG ───────────────────────────────
def _s3_to_pg(t: T, tpl=False):
    src = t.pg_table("src", ROWS)
    tgt = t.name("tgt")
    t.pg(_pg_create_sql(t.full(tgt), t.temp, incarnation=True, inc_col=False))
    t.pg(f"INSERT INTO {t.full(tgt)}_1 (id) VALUES (99)")  # лишняя строка: pre_truncate обязан её убрать
    # CSV самой PG: строка 2 с переводом строки в кавычках — row_count по rowcount COPY, а не по \n
    key = t.s3_put("src.csv.gz", gzip.compress(t.pg_copy_out(f"SELECT * FROM {t.full(src)}")))
    kw = dict(compression="gzip", pre_truncate=True, include_file_name=False, file_name_position=0)
    if tpl:
        kw = dict(compression="{{ 'gzip' }}", pre_truncate="{{ 'true' }}", include_file_name="{{ 'false' }}",
                  file_name_position="{{ 0 }}")
    # temp: оператор открывает соединение дважды — узнать инкарнацию и для COPY
    result, _ = t.run(sessions=("main", "main"), s3_bucket=t.bucket, s3_key=key, aws_conn_id=t.s3_conn,
                      target_conn_id=t.pg_conn, target_schema=t.pg_schema, target_table=tgt,
                      copy_csv_quote='"', **kw)
    active, got = t.pg_active(tgt)
    if active != 1 or got != t.pg_rows(src):
        raise AirflowFailException(f"активна инкарнация {active} (ждём 1) или строки не совпали: {got!r}"[:1000])
    t.expect_rows(result, len(got), required=True)


def s3_to_pg_gzip(t: T):
    """S3 → PG: COPY gzip-CSV в неактивную копию _1, перевод строки в кавычках, row_count = 3"""
    _s3_to_pg(t)


def s3_to_pg_tpl(t: T):
    """S3 → PG шаблоном: compression, pre_truncate, include_file_name, file_name_position из {{ }}"""
    _s3_to_pg(t, tpl=True)


# ──────────────────────────── Тесты: S3 → PG потоком ──────────────────────────
def _s3_to_pg_stream(t: T, name: str, data_sql: str, tpl=False, **kw):
    src = t.pg_table("src", ROWS)
    tgt = t.pg_table("tgt")
    q = f"SELECT * FROM {t.full(src)}"
    data = (t.pg_copy_out(q, header=True) if name.endswith(".csv")
            else t.pg(data_sql.format(q=q), fetch=True)[0][0].encode())
    key = t.s3_put(name, data)
    table, bucket = f"{t.pg_schema}.{tgt}", t.bucket
    if tpl:
        key, table, bucket = (f"{{{{ {v!r} }}}}" for v in (key, table, bucket))
    result, _ = t.run(sessions=("main",), s3_key=key, table=table, bucket=bucket, s3_conn_id=t.s3_conn,
                      conn_id=t.pg_conn, **kw)
    got = t.pg_rows(tgt)
    if got != t.pg_rows(src) or result.get("rows_loaded") != len(got):
        raise AirflowFailException(f"rows_loaded={result.get('rows_loaded')}, строки не совпали с источником: {got!r}"[:1000])


def s3_to_pg_stream_csv(t: T):
    """S3 → PG потоком из CSV с заголовком: строки = источник"""
    _s3_to_pg_stream(t, "src.csv", "")


def s3_to_pg_stream_json(t: T):
    """S3 → PG потоком из JSON-массива: раскладка по колонкам, строки = источник"""
    _s3_to_pg_stream(t, "src.json", "SELECT json_agg(s ORDER BY id)::text FROM ({q}) s")


def s3_to_pg_stream_tpl(t: T):
    """S3 → PG потоком шаблоном: s3_key, table, bucket из {{ }}"""
    _s3_to_pg_stream(t, "src.csv", "", tpl=True)


# ──────────────────────────── Тесты: CH → S3 ──────────────────────────────────
def _transport(t: T) -> str:
    # auto ходит по HTTP там, где файл выходит тот же; без HTTP у ClickHouse — только native
    return "auto" if t.context["params"]["ch_http"] else "native"


def _ch_to_s3(t: T, out: str, comp, **kw) -> bytes:
    """Выгрузка оператором в out[.ext] своей папки; один файл со сверкой хэша.

    comp — сжатие для имени и распаковки; оператору уходит kw["compression"] (шаблон), если он есть."""
    ext = {"gzip": ".gz", "zip": ".zip", "tar.gz": ".tar.gz"}.get(comp, "")
    _, spy = t.run(spy="verify_file_hash", s3_bucket=t.bucket, s3_key=f"{t.s3_prefix}{out}{ext}",
                   clickhouse_conn_id=t.ch_conn, aws_conn_id=t.s3_conn, replace=True, post_file_check=True,
                   **{"compression": comp, **kw})
    stem = out.rsplit(".", 1)[0]
    keys = t.s3_keys(stem)
    if len(keys) != 1 or spy.call_count != 1:
        raise AirflowFailException(f"ждём один файл {stem}… со сверкой хэша: {keys}, сверок {spy.call_count}")
    return _unpack(t.s3_get(keys[0]), comp)


def _rows_by_id(rows: list, ids=("1", "2", "3"), width=None) -> dict:
    """Строки файла по первой колонке; без заголовка; каждый id ровно раз (на CH порядок не гарантирован).

    width — полей в строке: неэкранированный таб или перевод строки разносит строку на лишние поля."""
    data = {r[0]: r for r in rows}
    if sorted(data) != sorted(ids) or len(data) != len(rows):
        raise AirflowFailException(f"id строк файла: {[r[:1] for r in rows]}, ждём {', '.join(ids)} по разу")
    if width and any(len(r) != width for r in rows):
        raise AirflowFailException(f"полей в строках {[len(r) for r in rows]}, ждём по {width}")
    return data


def _native_tsv(t: T, compression, sanitize=False, tpl=False):
    src = t.ch_table("src", rows=ROWS)
    kw = dict(sql=f"SELECT * FROM {src}", compression=compression, xstream_sanitize=sanitize, sanitize_array=sanitize)
    if tpl:
        kw = {k: f"{{{{ {v!r} }}}}" for k, v in kw.items()}
        kw["xstream_sanitize"] = kw["sanitize_array"] = "{{ 'false' }}"
    data = _ch_to_s3(t, "out.csv", compression, fmt="CSV", transport=_transport(t), **kw)
    rows = list(csv.reader(io.StringIO(data.decode()), **TSV))
    if rows[:1] != [COL_NAMES]:
        raise AirflowFailException(f"заголовок {rows[:1]!r}, ждём {COL_NAMES}")
    if t.pushed("row_count") != EXPECTED_ROWS:
        raise AirflowFailException(f"row_count={t.pushed('row_count')}, ждём {EXPECTED_ROWS}")
    return dict(zip(COL_NAMES, _rows_by_id(rows[1:], width=len(COL_NAMES))["2"]))


def ch_native_to_s3_gzip(t: T):
    """CH → S3, gzip: TSV с заголовком, три строки, оператор сверяет хэш"""
    _native_tsv(t, "gzip")


def ch_native_to_s3_zip(t: T):
    """CH → S3, zip: в архиве один TSV с заголовком"""
    _native_tsv(t, "zip")


def ch_native_to_s3_targz(t: T):
    """CH → S3, tar.gz: в архиве один TSV с заголовком"""
    _native_tsv(t, "tar.gz")


def ch_native_to_s3_none(t: T):
    """CH → S3 без сжатия: TSV с заголовком"""
    _native_tsv(t, None)


def ch_native_to_s3_sanitize(t: T):
    """CH → S3 с очисткой: xstream_sanitize и sanitize_array вырезают спецсимволы строки 2"""
    _check_sanitized(_native_tsv(t, "gzip", sanitize=True))


def ch_native_to_s3_tpl(t: T):
    """CH → S3 шаблоном: sql, compression и xstream_sanitize='false' из {{ }} — строка 2 без очистки"""
    row = _native_tsv(t, "gzip", tpl=True)
    if row["s_special"] != _SPECIAL:
        raise AirflowFailException(f"xstream_sanitize='false' из шаблона: s_special={row['s_special']!r}")


def _native_list(t: T, compression, tpl=False):
    src = t.ch_table("src", rows=ROWS)
    kw = dict(sql=f"SELECT * FROM {src}", compression=compression, fmt="JSON")
    if tpl:
        kw = {k: f"{{{{ {v!r} }}}}" for k, v in kw.items()}
    # у JSON Lines заголовка нет: с header=True (по умолчанию) native первой строкой пишет имена колонок
    data = _ch_to_s3(t, "out.json", compression, transport=_transport(t), header=False, **kw)
    rows = [json.loads(line) for line in data.decode().splitlines() if line.strip()]
    _rows_by_id([[str(r.get("id"))] for r in rows])
    if t.pushed("row_count_list") != [EXPECTED_ROWS]:
        raise AirflowFailException(f"row_count_list={t.pushed('row_count_list')}, ждём [{EXPECTED_ROWS}]")
    row2 = next((r for r in rows if str(r.get("id")) == "2"), None)
    if row2 is None:
        raise AirflowFailException("в JSON нет строки id=2")
    special = row2["s_special"]
    if special != _SPECIAL:
        raise AirflowFailException(f"s_special={special!r}, ждём {_SPECIAL!r}")


def ch_native_list_gzip(t: T):
    """CH → S3 частями, gzip: JSON Lines, файл out_1, три объекта, спецсимволы целы"""
    _native_list(t, "gzip")


def ch_native_list_zip(t: T):
    """CH → S3 частями, zip: в архиве один JSON Lines"""
    _native_list(t, "zip")


def ch_native_list_targz(t: T):
    """CH → S3 частями, tar.gz: в архиве один JSON Lines"""
    _native_list(t, "tar.gz")


def ch_native_list_none(t: T):
    """CH → S3 частями без сжатия: JSON Lines"""
    _native_list(t, None)


def ch_native_list_tpl(t: T):
    """CH → S3 частями шаблоном: sql, compression и fmt из {{ }}"""
    _native_list(t, "gzip", tpl=True)


def _ch_csv_ids(data: bytes) -> list:
    rows = list(csv.reader(io.StringIO(data.decode())))
    return rows[1:] if rows[:1] and rows[0][:1] == ["id"] else rows  # с заголовком или без — CSV клика


def _ch_table_to_s3(t: T, compression, tpl=False):
    src = t.ch_table("src", rows=ROWS)
    db, _, table = src.partition(".")
    kw = dict(table_name=table, schema=db)
    if tpl:
        kw = {k: f"{{{{ {v!r} }}}}" for k, v in kw.items()}
    _rows_by_id(_ch_csv_ids(_ch_to_s3(t, "out.csv", compression, **kw)))
    if t.pushed("row_count") != EXPECTED_ROWS:
        raise AirflowFailException(f"row_count={t.pushed('row_count')} (журнал запросов кластера), ждём {EXPECTED_ROWS}")


def ch_table_to_s3_gzip(t: T):
    """Таблица CH → S3, gzip: три строки, row_count по журналу запросов кластера datalab"""
    _ch_table_to_s3(t, "gzip")


def ch_table_to_s3_none(t: T):
    """Таблица CH → S3 без сжатия: три строки"""
    _ch_table_to_s3(t, None)


def ch_table_to_s3_tpl(t: T):
    """Таблица CH → S3 шаблоном: table_name и schema из {{ }}"""
    _ch_table_to_s3(t, "gzip", tpl=True)


def _ch_query_to_s3(t: T, tpl=False):
    src = t.ch_table("src", rows=ROWS)
    sql_ = f"SELECT id, name FROM {src} WHERE id > 1"
    _rows_by_id(_ch_csv_ids(_ch_to_s3(t, "out.csv", "gzip", sql=f"{{{{ {sql_!r} }}}}" if tpl else sql_)),
                ids=("2", "3"))
    if t.pushed("row_count") != 2:
        raise AirflowFailException(f"row_count={t.pushed('row_count')} (журнал запросов кластера), ждём 2")


def ch_query_to_s3_gzip(t: T):
    """Запрос CH → S3, gzip: две строки по условию, row_count по журналу запросов кластера datalab"""
    _ch_query_to_s3(t)


def ch_query_to_s3_tpl(t: T):
    """Запрос CH → S3 шаблоном: sql из {{ }}"""
    _ch_query_to_s3(t, tpl=True)


# ──────────────────────────── Тесты: CH ↔ PG ──────────────────────────────────
def _ch_to_pg(t: T, tpl=False, inc=False):
    src = t.ch_table("src", rows=ROWS)
    want = t.pg_table("want", ROWS)
    tgt = t.name("tgt")
    if inc:
        t.pg(_pg_create_sql(t.full(tgt), t.temp, incarnation=True))
    else:
        t.pg_table("tgt")
    kw = dict(sql=f"SELECT {', '.join(COL_NAMES)} FROM {src}", target_table=tgt, target_schema=t.pg_schema)
    if tpl:
        kw = {k: f"{{{{ {v!r} }}}}" for k, v in kw.items()}
    result, _ = t.run(sessions=("main",), clickhouse_conn_id=t.ch_conn, target_conn_id=t.pg_conn,
                      **kw, **({} if inc else {"truncate": True}))
    active, got = t.pg_active(tgt) if inc else (1, t.pg_rows(tgt))
    exp = t.pg_rows(want)
    # float — с допуском: клик по умолчанию парсит литерал быстро (precise_float_parsing=0), 4.56 → 4.5600000000000005
    diff = [(r[0], c, g, w) for r, e in zip(got, exp) for c, g, w in zip(COL_NAMES, r, e)
            if not (g == w or isinstance(g, float) and isinstance(w, float) and math.isclose(g, w))]
    if active != 1 or len(got) != len(exp) or diff:
        raise AirflowFailException(f"активна инкарнация {active} (ждём 1), строк {len(got)} из {len(exp)}; "
                                   f"(id, колонка, в цели, в источнике): {diff!r}"[:1000])
    t.expect_rows(result, len(got))


def ch_to_pg_full(t: T):
    """CH → PG: перелив запроса с truncate, строки = источник (пустая строка остаётся пустой, не NULL)"""
    _ch_to_pg(t)


def ch_to_pg_tpl(t: T):
    """CH → PG шаблоном: sql, target_table и target_schema из {{ }}"""
    _ch_to_pg(t, tpl=True)


def ch_to_pg_inc_full(t: T):
    """CH → PG в инкарнацию: перелив в неактивную копию _1 и переключение"""
    _ch_to_pg(t, inc=True)


def ch_to_pg_inc_tpl(t: T):
    """CH → PG в инкарнацию шаблоном: sql, target_table и target_schema из {{ }}"""
    _ch_to_pg(t, tpl=True, inc=True)


def pg_to_ch_table(t: T):
    """PG → CH: таблица в строковую таблицу CH через CSV, сверка числа строк оператором, спецсимволы целы"""
    src = t.pg_table("src", ROWS)
    tgt = t.ch_table("tgt", typed=False)
    db, _, table = tgt.partition(".")
    t.run(sessions=("main",), source_conn_id=t.pg_conn, source_schema=t.pg_schema, source_table=src,
          target_conn_id=t.ch_conn, target_schema=db, target_table=table, target_truncate=True,
          target_fmt="CSV", post_count_check=True)
    (cnt,), = t.ch(f"SELECT count() FROM {tgt}")
    special = t.ch(f"SELECT s_special FROM {tgt} WHERE id = '2'")
    if cnt != EXPECTED_ROWS or t.pushed("row_count") != cnt or special != [(_SPECIAL,)]:
        raise AirflowFailException(f"в цели {cnt} строк, row_count={t.pushed('row_count')}, s_special={special!r}")


# ──────────────────────────── Тесты: CH → CH ──────────────────────────────────
def _ch_to_ch(t: T, cols="id, name, s_special", typed=False, tpl=False, **kw):
    src = t.ch_table("src", rows=ROWS)
    if typed:
        tgt = t.ch_table("tgt")
    else:
        # три колонки: JSONEachRow отдаёт даты строками, а типизированную вставку дат clickhouse_connect
        # не делает — здесь проверяется сам поток, приведения — в случае typed
        tgt = t.ch_full(t.name("tgt"))
        t.ch(f"CREATE TABLE {tgt} (id Int32, name String, s_special String) ENGINE = MergeTree() ORDER BY id")
    t.ch(f"INSERT INTO {tgt} (id, name, s_special) VALUES (99, 'лишняя', '')")  # truncate_target её уберёт
    names = {"source_sql": f"SELECT {cols} FROM {src}", "target_table": tgt}
    kw = {**({k: f"{{{{ {v!r} }}}}" for k, v in names.items()} if tpl else names), **kw}
    result, _ = t.run(clickhouse_source_conn_id=t.ch_conn, clickhouse_target_conn_id=t.ch_conn, heimdall_conn_id=None,
                      truncate_target=True, **kw)
    cnt, sid = t.ch(f"SELECT count(), sum(id) FROM {tgt}")[0]
    special = t.ch(f"SELECT s_special FROM {tgt} WHERE id = 2")
    if cnt != EXPECTED_ROWS or sid != 6 or special != [(_SPECIAL,)] or (result or {}).get("total_rows") != cnt:
        raise AirflowFailException(f"в цели count={cnt}, sum(id)={sid}, s_special={special!r}, результат {result!r}")
    if typed and t.ch(f"SELECT * FROM {tgt} ORDER BY id") != t.ch(f"SELECT * FROM {src} ORDER BY id"):
        raise AirflowFailException("типизированная цель отличается от источника построчно")


def ch_to_ch_stream(t: T):
    """CH → CH: поток в таблицу из трёх колонок порциями по 2, truncate_target убирает лишнюю строку"""
    _ch_to_ch(t, batch_size=2)


def ch_to_ch_typed(t: T):
    """CH → CH типизированно: SELECT * в таблицу с типами источника, цель = источник построчно"""
    _ch_to_ch(t, cols="*", typed=True)


def ch_to_ch_tpl(t: T):
    """CH → CH шаблоном: source_sql, target_table и batch_size='2' из {{ }}"""
    _ch_to_ch(t, batch_size="{{ 2 }}", tpl=True)


# ──────────────────────────── Тесты: кластер CH ───────────────────────────────
def _ch_cluster(t: T, cluster="datalab"):
    # DDL идёт на каждый узел своим подключением click-dlab-<узел>; таблица — с префиксом теста:
    # упавший посреди тест оставит её на узлах, повтор пройдёт (IF NOT EXISTS / IF EXISTS)
    tbl = t.ch_full(t.name("probe"))
    nodes = t.systems["ch"]["cluster"]["nodes"]
    for sql_ in (f"CREATE TABLE IF NOT EXISTS {tbl} (id Int32) ENGINE = MergeTree() ORDER BY id",
                 f"DROP TABLE IF EXISTS {tbl}"):
        res, _ = t.run(sql=sql_, clickhouse_conn_id=t.ch_conn, cluster_name=cluster)
        ok = sorted(r["node"] for r in res or [] if r.get("success"))
        if ok != sorted(nodes):
            raise AirflowFailException(f"{sql_.split(' (')[0]}: выполнено на {ok}, узлы кластера {sorted(nodes)}")


def ch_cluster_ddl(t: T):
    """Кластер CH: CREATE и DROP таблицы на каждом узле datalab"""
    _ch_cluster(t)


def ch_cluster_tpl(t: T):
    """Кластер CH шаблоном: cluster_name из {{ }}"""
    _ch_cluster(t, cluster="{{ 'datalab' }}")


# ──────────────────────────── Тесты: S3 → CH ──────────────────────────────────
def _s3_to_ch_table(t: T, name, data, fmt, **kw):
    tgt = t.ch_table("tgt", typed=False)
    t.ch(f"INSERT INTO {tgt} (id) VALUES ('99')")  # truncate её уберёт
    key = t.s3_put(name, gzip.compress(data))
    db, _, table = tgt.partition(".")
    kw = {"truncate": True, **kw}
    t.run(table_name=table, schema=db, s3_bucket=t.bucket, s3_key=key, clickhouse_conn_id=t.ch_conn,
          aws_conn_id=t.s3_conn, fmt=fmt, compression="gzip", **kw)
    (cnt,), = t.ch(f"SELECT count() FROM {tgt}")
    special = t.ch(f"SELECT s_special FROM {tgt} WHERE id = '2'")
    if cnt != EXPECTED_ROWS or special != [(_SPECIAL,)]:
        raise AirflowFailException(f"в цели {cnt} строк (ждём {EXPECTED_ROWS}), s_special={special!r}")
    t.expect_rows({"row_count": t.pushed("row_count")}, cnt, required=True)


def s3_to_ch_table_csv(t: T):
    """S3 → CH из CSV без заголовка: перевод строки в кавычках, truncate убирает лишнюю строку"""
    _s3_to_ch_table(t, "src.csv.gz", _csv_bytes(header=False), "CSV")


def s3_to_ch_table_tsv(t: T):
    """S3 → CH из TSVWithNames: экраны \\t \\n \\\\ раскрываются, строки = источник"""
    _s3_to_ch_table(t, "src.tsv.gz", _tsv_bytes(), "TSVWithNames")


def s3_to_ch_table_tpl(t: T):
    """S3 → CH шаблоном: truncate='true' и settings JSON-строкой из {{ }}"""
    _s3_to_ch_table(t, "src.csv.gz", _csv_bytes(header=False), "CSV", truncate="{{ 'true' }}",
                    settings="{{ '%s' }}" % json.dumps({"input_format_csv_empty_as_default": 1}))


def _s3_to_ch_transformed(t: T, **kw):
    # Transformed читает TSV без экранов (формат пакетной выгрузки в ПКАП): табов, переводов строк и
    # обратных слешей в значениях быть не должно — вход как после xstream_sanitize
    rows = [tuple(SANITIZED_SPECIAL if c == "s_special" and r[0] == 2 else v for c, v in zip(COL_NAMES, r))
            for r in ROWS]
    tgt = t.ch_table("tgt", typed=False, lead="file_name String")
    key = t.s3_put("src.tsv.gz", gzip.compress(_tsv_bytes(rows, esc=lambda s: s.replace("\\", ""))))
    t.ch(f"INSERT INTO {tgt} (file_name, id) VALUES ('old', '99')")  # truncate обязан её убрать
    db, _, table = tgt.partition(".")
    t.run(table_name=table, schema=db, s3_bucket=t.bucket, s3_key=key, clickhouse_conn_id=t.ch_conn,
          aws_conn_id=t.s3_conn, fmt="TabSeparatedWithNames", compression="gzip", **kw)
    got = t.ch(f"SELECT file_name, count(), countIf(id = '2' AND s_special = '{SANITIZED_SPECIAL}') "
               f"FROM {tgt} GROUP BY file_name")
    if got != [("src.tsv.gz", EXPECTED_ROWS, 1)]:
        raise AirflowFailException(f"(file_name, строк, строка 2 цела): {got!r}, ждём [('src.tsv.gz', 3, 1)]")
    t.expect_rows({"row_count": t.pushed("row_count")}, EXPECTED_ROWS, required=True)  # заголовок — не строка


def s3_to_ch_transformed_tsv(t: T):
    """S3 → CH с именем файла: TSV пакетной выгрузки, имя файла первой колонкой"""
    _s3_to_ch_transformed(t, truncate=True, include_file_name=True, file_name_position=0)


def s3_to_ch_transformed_tpl(t: T):
    """S3 → CH с именем файла шаблоном: truncate, include_file_name, file_name_position из {{ }}"""
    _s3_to_ch_transformed(t, truncate="{{ 'true' }}", include_file_name="{{ 'true' }}", file_name_position="{{ 0 }}")


# ──────────────────────────── Тесты: S3-утилиты ────────────────────────────────
def _s3_archive(t: T, post_file_check):
    parts = {"a.csv": _csv_bytes(), "b.csv": _csv_bytes(header=False)}
    keys = [t.s3_put(n, d) for n, d in parts.items()]
    tgt = f"{t.s3_prefix}bundle.zip"
    _, spy = t.run(spy="verify_file_hash", s3_keys_source=keys, s3_bucket_source=t.bucket,
                   aws_conn_id_source=t.s3_conn, s3_bucket=t.bucket, s3_key=tgt, aws_conn_id=t.s3_conn,
                   replace=True, post_file_check=post_file_check)
    with zipfile.ZipFile(io.BytesIO(t.s3_get(tgt))) as z:
        got = sorted(z.read(n) for n in z.namelist())
    if got != sorted(parts.values()):
        raise AirflowFailException(f"в архиве {len(got)} файлов, содержимое не совпало с исходными")
    return spy


def s3_archive_two(t: T):
    """Архив S3: два файла упаковываются в один zip байт в байт, post_file_check сверяет хэш архива"""
    spy = _s3_archive(t, True)
    if spy.call_count != 1:
        raise AirflowFailException(f"post_file_check=True не подействовал: сверок {spy.call_count}")


def s3_archive_tpl(t: T):
    """Архив S3 шаблоном: post_file_check='False' из {{ }} — ложь, а не непустая строка: сверки нет"""
    spy = _s3_archive(t, "{{ 'False' }}")
    if spy.call_count:
        raise AirflowFailException(f"post_file_check='False' из шаблона сочтён истиной: сверок {spy.call_count}")


def _s3_file_hash(t: T, checksum: str, tpl=False):
    raw = _csv_bytes()
    want = hashlib.md5(raw).hexdigest()  # noqa: S324 — так считает оператор
    key = t.s3_put("src.csv.gz", gzip.compress(raw))
    checksum = checksum or want
    if tpl:  # шаблонные поля оператора — строки: checksum, s3_key (compression не шаблонизируется)
        key, checksum = f"{{{{ {key!r} }}}}", f"{{{{ {checksum!r} }}}}"
    t.run(s3_bucket=t.bucket, s3_key=key, aws_conn_id=t.s3_conn, compression="gzip", checksum=checksum)
    return t.pushed("checksum"), want


def s3_file_hash_ok(t: T):
    """Хэш файла S3: md5 распакованного gzip совпадает, оператор кладёт его в XCom"""
    got, want = _s3_file_hash(t, "")
    if got != want:
        raise AirflowFailException(f"checksum в XCom {got}, ждём {want}")


def s3_file_hash_mismatch(t: T):
    """Хэш файла S3: чужой хэш — оператор падает, а не пропускает файл"""
    try:
        _s3_file_hash(t, "0" * 32)
    except AirflowFailException as e:
        if "does not match" not in str(e):
            raise
        return
    raise AirflowFailException("чужой хэш принят")


def s3_file_hash_tpl(t: T):
    """Хэш файла S3 шаблоном: s3_key и checksum из {{ }}"""
    got, want = _s3_file_hash(t, "", tpl=True)
    if got != want:
        raise AirflowFailException(f"checksum в XCom {got}, ждём {want}")


def _s3_list_keys(t: T, **kw) -> list:
    for i in range(5):
        t.s3_put(f"k{i}.txt", b"x")
    result, _ = t.run(bucket=t.bucket, prefix=t.s3_prefix, aws_conn_id=t.s3_conn, **kw)
    return [r.get("Key") if isinstance(r, dict) else r for r in result or []]


def s3_list_keys_all(t: T):
    """Список ключей S3: все пять своих ключей"""
    got = _s3_list_keys(t)
    if sorted(got) != [f"{t.s3_prefix}k{i}.txt" for i in range(5)]:
        raise AirflowFailException(f"ключи: {got}")


def s3_list_keys_tpl(t: T):
    """Список ключей S3 шаблоном: max_items='2' из {{ }} — список урезан"""
    got = _s3_list_keys(t, max_items="{{ 2 }}")
    # оператор запрашивает max_items + 1 — лишний ключ нужен ему для признака has_next
    if got != [f"{t.s3_prefix}k{i}.txt" for i in range(3)]:
        raise AirflowFailException(f"max_items='2' из шаблона: {got} (ждём 2 + 1 для has_next из 5)")


def _s3_file_read(t: T, rows) -> list:
    key = t.s3_put("src.csv.gz", gzip.compress(_csv_bytes()))
    got, orig = [], t.cls.read_rows

    def read_rows(self, *a, **kw):  # оператор только пишет строки в лог — перехватываем, что он прочёл
        got.append(orig(self, *a, **kw))
        return got[-1]

    with mock.patch.object(t.cls, "read_rows", read_rows):
        t.run(s3_bucket=t.bucket, s3_key=key, aws_conn_id=t.s3_conn, rows=rows)
    if not got:
        raise AirflowFailException("read_rows не вызван или прерван лимитом строк")
    return got[0][0]


def s3_file_read_rows(t: T):
    """Чтение файла S3: первые две строки gzip-файла — заголовок и первая запись"""
    got = _s3_file_read(t, 2)
    want = _csv_bytes().decode().splitlines()[:2]
    if got != want:
        raise AirflowFailException(f"прочитано {got!r}"[:500])


def s3_file_read_tpl(t: T):
    """Чтение файла S3 шаблоном: rows='1' из {{ }} — одна строка"""
    got = _s3_file_read(t, "{{ 1 }}")
    # не только число: на пустом потоке read_rows всё равно вернёт rows пустых строк
    if got != _csv_bytes().decode().splitlines()[:1]:
        raise AirflowFailException(f"rows='1' из шаблона: прочитано {got!r}"[:500])


def _s3_bucket_viewer(t: T, conn):
    result, _ = t.run(aws_conn_id=conn)
    names = [b.get("Name") for b in result or []]
    if t.bucket not in names:
        raise AirflowFailException(f"бакета {t.bucket} нет в списке {names}"[:500])


def s3_bucket_viewer_list(t: T):
    """Обзор бакетов S3: бакет из формы в списке подключения"""
    _s3_bucket_viewer(t, t.s3_conn)


def s3_bucket_viewer_tpl(t: T):
    """Обзор бакетов S3 шаблоном: aws_conn_id из {{ }}"""
    _s3_bucket_viewer(t, f"{{{{ {t.s3_conn!r} }}}}")


# ─────────────────────────────── Тесты: lineage ───────────────────────────────
def lineage_sql(t: T):
    """Lineage из SQL: inlets и outlets — Asset Airflow 3 (утилита пакета, систем не трогает)"""
    from airflow.sdk import Asset

    src, tgt = f"{t.ch_db}.{t.name('src')}", f"{t.ch_db}.{t.name('tgt')}"
    inlets, outlets = t.cls(f"INSERT INTO {tgt} SELECT id, name FROM {src}", t.ch_conn)
    got = ([a.uri for a in inlets], [a.uri for a in outlets])
    want = ([f"table://{t.ch_conn}/{src.replace('.', '/')}"], [f"table://{t.ch_conn}/{tgt.replace('.', '/')}"])
    if got != want or not all(isinstance(a, Asset) for a in inlets + outlets):
        raise AirflowFailException(f"lineage: получено {got}, ожидалось {want}")


# ─────────────────────────── Тесты: стандартные — Python ──────────────────────────
def _ctx_fn(a, b=0, **kw):
    """Вызываемое Python-операторов: аргументы и контекст рана (ti, run_id)."""
    return {"a": a, "b": b, "ti": kw["ti"].task_id, "run_id": kw["run_id"]}


def _python(t: T, b, want_b):
    result, _ = t.run(python_callable=_ctx_fn, op_args=[1], op_kwargs={"b": b})
    want = {"a": 1, "b": want_b, "ti": t.context["ti"].task_id, "run_id": t.context["run_id"]}
    if result != want:
        raise AirflowFailException(f"вернула {result}, ждём {want}")


def empty_run(t: T):
    """EmptyOperator: выполняется без ошибки и ничего не возвращает"""
    result, _ = t.run()
    if result is not None:
        raise AirflowFailException(f"вернул {result!r}")


def python_args(t: T):
    """PythonOperator: op_args, op_kwargs и контекст рана в функции"""
    _python(t, 2, 2)


def python_tpl(t: T):
    """PythonOperator шаблоном: op_kwargs из {{ }} — строка '42'"""
    _python(t, "{{ 40 + 2 }}", "42")


def task_args(t: T):
    """@task: op_args, op_kwargs и контекст рана в функции"""
    _python(t, 2, 2)


def task_tpl(t: T):
    """@task шаблоном: op_kwargs из {{ }} — строка '42'"""
    _python(t, "{{ 40 + 2 }}", "42")


# ─────────────────────── Тесты: стандартные — ветвление, сенсоры ──────────────────
SENSOR_TIMEOUT = 10  # с: тест «нет» ждёт его целиком


@contextmanager
def _no_skip(t: T):
    """Ветвление AF3 пропускает downstream задачи из контекста — это задача-тест и следующий тест группы.
    Подменяем пропуск на классе оператора и сверяем, что он запросил."""
    with mock.patch.object(t.cls, "skip_all_except") as m_all, mock.patch.object(t.cls, "skip") as m_skip:
        yield m_all, m_skip


def _ids(x) -> list[str]:
    return [x] if isinstance(x, str) else sorted(x or [])


def _branch(t: T, flag, want: str, **kw):
    """Ветка выбрана по flag: execute вернул её, пропуск запрошен «всё, кроме неё»."""
    with _no_skip(t) as (m_all, _):
        result, _ = t.run(**kw)
    asked = _ids(m_all.call_args.args[1]) if m_all.called else None
    if _ids(result) != [want] or asked != [want]:
        raise AirflowFailException(f"flag={flag!r}: вернул {result!r}, пропуск кроме {asked}, ждём {want}")


def _pick(flag):
    """Вызываемое ветвления: ветка — существующая задача дага (иначе skip_all_except отвергнет id)."""
    return "systems" if flag in (True, "True") else "report"


SKIP_GRAPH = "test_standart.skip_graph."
# служебные задачи подгруппы: в verdict и сводке не считаются, тест — только check
GRAPH_TASKS = {SKIP_GRAPH + x for x in ("branch", "keep", "drop", "short", "after")}
GRAPH_WANT = {"branch": "success", "keep": "success", "drop": "skipped", "short": "success", "after": "skipped"}


def _graph_branch():
    return SKIP_GRAPH + "keep"


def _sensor_ok(t: T, **kw):
    """Условие выполнено — сенсор выходит за первый poke."""
    _, m = t.run(mode="poke", poke_interval=1, timeout=SENSOR_TIMEOUT, spy="poke", **kw)
    if m.call_count != 1:
        raise AirflowFailException(f"poke вызван {m.call_count} раз, ждём 1")


def _sensor_timeout(t: T, **kw):
    """Условие не выполнено — сенсор падает по своему timeout, не раньше и не сильно позже."""
    started = time.monotonic()
    try:
        t.run(mode="poke", poke_interval=1, timeout=SENSOR_TIMEOUT, **kw)
    except AirflowSensorTimeout:
        took = time.monotonic() - started
        if not SENSOR_TIMEOUT <= took <= SENSOR_TIMEOUT + 20:
            raise AirflowFailException(f"таймаут через {took:.1f} с, ждём ~{SENSOR_TIMEOUT}") from None
        return
    raise AirflowFailException("сенсор вышел без AirflowSensorTimeout")


def branch_python_both(t: T):
    """BranchPythonOperator: обе ветки по условию, задачи регресса не пропускаются"""
    for flag, want in ((True, "systems"), (False, "report")):
        _branch(t, flag, want, python_callable=_pick, op_kwargs={"flag": flag})


def branch_python_tpl(t: T):
    """BranchPythonOperator шаблоном: условие из {{ }} — строка 'True'"""
    _branch(t, "{{ 1 == 1 }}", "systems", python_callable=_pick, op_kwargs={"flag": "{{ 1 == 1 }}"})


def _short(t: T, flag, want):
    with _no_skip(t) as (m_all, m_skip):
        result, _ = t.run(python_callable=lambda flag: flag, op_kwargs={"flag": flag})
    # у оператора в тесте нет downstream: «нет» не должно ничего пропускать
    if result != want or m_all.called or m_skip.called:
        raise AirflowFailException(f"flag={flag!r}: вернул {result!r}, skip={m_skip.called or m_all.called}")


def short_circuit_both(t: T):
    """ShortCircuitOperator: «да» и «нет», задачи регресса не пропускаются"""
    _short(t, True, True)
    _short(t, False, False)


def short_circuit_tpl(t: T):
    """ShortCircuitOperator шаблоном: условие из {{ }} — строка 'True'"""
    _short(t, "{{ 1 == 1 }}", "True")


def task_sensor_ok(t: T):
    """@task.sensor: условие выполнено — выход за первый poke"""
    _sensor_ok(t, python_callable=lambda: True)


def task_sensor_timeout(t: T):
    """@task.sensor: условие не выполнено — падение по timeout"""
    _sensor_timeout(t, python_callable=lambda: False)


def task_sensor_tpl(t: T):
    """@task.sensor шаблоном: op_kwargs из {{ }} доходит до функции"""
    _sensor_ok(t, python_callable=lambda x: x == "ok", op_kwargs={"x": "{{ 'o' ~ 'k' }}"})


# ─────────────────────────── Тесты: стандартные — SQL и CH ────────────────────────
_IDS = sorted(r[0] for r in ROWS)  # id — первая колонка COLUMNS


def _sql_select(t: T, sql, params, want):
    src = t.full(t.pg_table("src", ROWS))
    result, _ = t.run(conn_id=t.pg_conn, sql=sql.format(src=src), parameters=params, sessions=["main"])
    got = [tuple(r) for r in result or []]
    if got != want:
        raise AirflowFailException(f"получено {got}, ждём {want}")


def sql_execute_select(t: T):
    """SQLExecuteQueryOperator: SELECT с parameters по таблице теста на pg формы"""
    _sql_select(t, "SELECT id FROM {src} WHERE id <= %(n)s ORDER BY id", {"n": _IDS[1]}, [(i,) for i in _IDS[:2]])


def sql_execute_tpl(t: T):
    """SQLExecuteQueryOperator шаблоном: SQL из {{ }}"""
    _sql_select(t, "SELECT count(*) FROM {src} WHERE id <= {{{{ %d }}}}" % _IDS[1], None, [(2,)])


def ch_operator_rows(t: T):
    """ClickHouseOperator: DDL, INSERT … SELECT с parameters и SELECT одним списком"""
    src, tgt = t.ch_table("src", rows=ROWS), t.ch_full(t.name("tgt"))
    result, _ = t.run(clickhouse_conn_id=t.ch_conn, parameters={"n": _IDS[1]}, sql=[
        f"CREATE TABLE {tgt} (id Int64) ENGINE = MergeTree ORDER BY id",
        f"INSERT INTO {tgt} SELECT id FROM {src} WHERE id <= %(n)s",
        f"SELECT id FROM {tgt} ORDER BY id"])
    if [int(r[0]) for r in result or []] != _IDS[:2]:
        raise AirflowFailException(f"получено {result}, ждём {_IDS[:2]}")


def ch_operator_tpl(t: T):
    """ClickHouseOperator шаблоном: SQL из {{ }}"""
    result, _ = t.run(clickhouse_conn_id=t.ch_conn, sql="SELECT {{ 40 + 2 }}")
    if result != [(42,)]:
        raise AirflowFailException(f"получено {result}")


def ch_branch_sql_both(t: T):
    """ClickHouseBranchSQLOperator: обе ветки по результату SQL, задачи регресса не пропускаются"""
    for sql, want in (("SELECT 1", "systems"), ("SELECT 0", "report")):
        with _no_skip(t) as (m_all, _):
            t.run(conn_id=t.ch_conn, database=t.ch_db, sql=sql,
                  follow_task_ids_if_true="systems", follow_task_ids_if_false="report")
        asked = _ids(m_all.call_args.args[1]) if m_all.called else None
        if asked != [want]:
            raise AirflowFailException(f"{sql}: пропуск кроме {asked}, ждём {want}")


def ch_branch_sql_tpl(t: T):
    """ClickHouseBranchSQLOperator шаблоном: SQL из {{ }}"""
    with _no_skip(t) as (m_all, _):
        t.run(conn_id=t.ch_conn, database=t.ch_db, sql="SELECT {{ 1 }}",
              follow_task_ids_if_true="systems", follow_task_ids_if_false="report")
    if not m_all.called or _ids(m_all.call_args.args[1]) != ["systems"]:
        raise AirflowFailException(f"пропуск: {m_all.call_args}")


def ch_sensor_ok(t: T):
    """ClickHouseSensor шаблоном: строки есть — выход за первый poke"""
    src = t.ch_table("src", rows=ROWS)
    _sensor_ok(t, clickhouse_conn_id=t.ch_conn, sql=f"SELECT count() FROM {src} WHERE id > {{{{ 0 }}}}",
               is_success=lambda r: r[0][0] == len(ROWS))


def ch_sensor_timeout(t: T):
    """ClickHouseSensor: условие не выполнено — падение по timeout"""
    _sensor_timeout(t, clickhouse_conn_id=t.ch_conn, sql="SELECT 0", is_success=lambda r: bool(r[0][0]))


def _ch_last(result):
    """Строки последнего запроса: ядро ставит перед SQL `SET log_comment=…;` (ClickHouseLogCommentMixin), строка
    становится списком из двух запросов, и return_last не действует — даги платформы получают `[[], строки]`"""
    return result[-1] if result and isinstance(result[0], list) else result


def ch_sql_execute_select(t: T):
    """ClickHouseSQLExecuteQueryOperator: SELECT с parameters по таблице теста"""
    src = t.ch_table("src", rows=ROWS)
    result, _ = t.run(conn_id=t.ch_conn, database=t.ch_db, parameters={"n": _IDS[1]},
                      sql=f"SELECT id FROM {src} WHERE id <= %(n)s ORDER BY id")
    result = _ch_last(result)
    if [int(r[0]) for r in result or []] != _IDS[:2]:
        raise AirflowFailException(f"получено {result}, ждём {_IDS[:2]}")


def ch_sql_execute_tpl(t: T):
    """ClickHouseSQLExecuteQueryOperator шаблоном: SQL из {{ }}"""
    result, _ = t.run(conn_id=t.ch_conn, database=t.ch_db, sql="SELECT {{ 40 + 2 }}")
    result = _ch_last(result)
    if [tuple(r) for r in result or []] != [(42,)]:
        raise AirflowFailException(f"получено {result}")


def task_branch_both(t: T):
    """@task.branch: обе ветки по условию, задачи регресса не пропускаются"""
    for flag, want in ((True, "systems"), (False, "report")):  # декорированному op_args обязателен
        _branch(t, flag, want, python_callable=_pick, op_args=[], op_kwargs={"flag": flag})


def task_branch_tpl(t: T):
    """@task.branch шаблоном: условие из {{ }} — строка 'True'"""
    _branch(t, "{{ 1 == 1 }}", "systems", python_callable=_pick, op_args=[], op_kwargs={"flag": "{{ 1 == 1 }}"})


def _branch_sql(t: T, sql: str, want: str, **kw):
    with _no_skip(t) as (m_all, _):
        t.run(conn_id=t.pg_conn, sql=sql, follow_task_ids_if_true="systems", follow_task_ids_if_false="report", **kw)
    asked = _ids(m_all.call_args.args[1]) if m_all.called else None
    if asked != [want]:
        raise AirflowFailException(f"{sql}: пропуск кроме {asked}, ждём {want}")


def branch_sql_both(t: T):
    """BranchSQLOperator: обе ветки по результату SQL с parameters на pg формы"""
    src = t.full(t.pg_table("src", ROWS))
    for n, want in ((_IDS[0], "systems"), (_IDS[0] - 1, "report")):
        _branch_sql(t, f"SELECT count(*) FROM {src} WHERE id <= %(n)s", want, parameters={"n": n}, sessions=["main"])


def branch_sql_tpl(t: T):
    """BranchSQLOperator шаблоном: SQL из {{ }}"""
    _branch_sql(t, "SELECT {{ 1 }}", "systems")


# ─────────────────────────────── Тесты: стандартные — S3 ──────────────────────────
def _s3_delete(t: T, **kw):
    for n in ("del/a.csv", "del/b.csv"):
        t.s3_put(n, b"x")
    t.run(aws_conn_id=t.s3_conn, bucket=t.bucket, **kw)
    if left := t.s3_keys("del/"):
        raise AirflowFailException(f"не удалены: {left}")


def s3_delete_keys(t: T):
    """S3DeleteObjectsOperator: удаление списком keys"""
    _s3_delete(t, keys=[t.s3_prefix + "del/a.csv", t.s3_prefix + "del/b.csv"])


def s3_delete_prefix(t: T):
    """S3DeleteObjectsOperator: удаление по prefix"""
    _s3_delete(t, prefix=t.s3_prefix + "del/")


def s3_delete_tpl(t: T):
    """S3DeleteObjectsOperator шаблоном: prefix из {{ }}"""
    _s3_delete(t, prefix=f"{{{{ {t.s3_prefix + 'del/'!r} }}}}")


def s3_key_sensor_ok(t: T):
    """S3KeySensor: ключ есть — выход за первый poke"""
    key = t.s3_put("ready.csv", b"x")
    _sensor_ok(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=key)


def s3_key_sensor_timeout(t: T):
    """S3KeySensor: ключа нет — падение по timeout"""
    _sensor_timeout(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=t.s3_prefix + "absent.csv")


def s3_key_sensor_tpl(t: T):
    """S3KeySensor шаблоном: bucket_key из {{ }}"""
    key = t.s3_put("ready.csv", b"x")
    _sensor_ok(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=f"{{{{ {key!r} }}}}")


def _s3_create_bucket(t: T, name):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    with mock.patch.object(S3Hook, "create_bucket") as create:  # бакеты регресс не создаёт
        t.run(aws_conn_id=t.s3_conn, bucket_name=name)
    if create.called or not t.s3.check_for_bucket(t.bucket):
        raise AirflowFailException(f"create_bucket вызван={create.called}: оператор не увидел бакет {t.bucket}")


def s3_create_bucket_exists(t: T):
    """S3CreateBucketOperator: бакет формы уже есть — без ошибки и без создания"""
    _s3_create_bucket(t, t.bucket)


def s3_create_bucket_tpl(t: T):
    """S3CreateBucketOperator шаблоном: bucket_name из {{ }}"""
    _s3_create_bucket(t, f"{{{{ {t.bucket!r} }}}}")


def _s3_copy(t: T, dst: str):
    src = t.s3_put("src.csv", b"id\n1\n")
    t.run(aws_conn_id=t.s3_conn, source_bucket_name=t.bucket, source_bucket_key=src,
          dest_bucket_name=t.bucket, dest_bucket_key=dst)
    if (got := t.s3.read_key(t.s3_prefix + "dst.csv", t.bucket)) != "id\n1\n":
        raise AirflowFailException(f"в копии {got!r}")


def s3_copy_key(t: T):
    """S3CopyObjectOperator: копия ключа теста в своей папке"""
    _s3_copy(t, t.s3_prefix + "dst.csv")


def s3_copy_tpl(t: T):
    """S3CopyObjectOperator шаблоном: dest_bucket_key из {{ }}"""
    _s3_copy(t, f"{{{{ {t.s3_prefix + 'dst.csv'!r} }}}}")


def _s3_create_object(t: T, data, want):
    key = t.s3_prefix + "obj.csv"
    t.run(aws_conn_id=t.s3_conn, s3_bucket=t.bucket, s3_key=key, data=data, replace=True)
    if (got := t.s3.read_key(key, t.bucket)) != want:
        raise AirflowFailException(f"записано {got!r}, ждём {want!r}")


def s3_create_object_data(t: T):
    """S3CreateObjectOperator: ключ из data"""
    # data — шаблонное поле: Jinja срезает завершающий перевод строки, без него — как в бою
    _s3_create_object(t, "id\n1", "id\n1")


def s3_create_object_tpl(t: T):
    """S3CreateObjectOperator шаблоном: data из {{ }}"""
    _s3_create_object(t, "{{ 40 + 2 }}", "42")


# ─────────────────────────── Тесты: стандартные — TriggerDagRun ───────────────────
DUMMY = "tools_dummy"


def trigger_dagrun_conf(t: T):
    """TriggerDagRunOperator шаблоном: ран tools_dummy с conf теста, без ожидания"""
    import structlog
    from airflow.sdk.exceptions import DagRunTriggerException
    from airflow.sdk.execution_time.task_runner import _handle_trigger_dag_run

    ti = t.context["ti"]
    # ponytail: «нет ранов» = «нет дага» — tools_dummy ежечасный; точнее — запрос DagBag через API
    if not ti.get_dr_count(dag_id=DUMMY):
        raise AirflowSkipException(f"нет дага {DUMMY} (нет ни одного рана)")
    run_id = f"{t.prefix}__{t.context['run_id']}__{ti.try_number}"[:250]
    try:
        t.run(trigger_dag_id=f"{{{{ {DUMMY!r} }}}}", trigger_run_id=run_id,
              conf={"rg": "{{ ti.task_id }}"}, wait_for_completion=False)
    except DagRunTriggerException as e:
        drte = e
    else:
        raise AirflowFailException("оператор не бросил DagRunTriggerException")
    if (drte.trigger_dag_id, drte.conf) != (DUMMY, {"rg": ti.task_id}):
        raise AirflowFailException(f"запрошено {drte.trigger_dag_id} conf={drte.conf}")
    # ран создаёт исполнитель задачи по исключению; при успехе — SUCCESS, при существующем ране — FAILED
    _, state = _handle_trigger_dag_run(drte, t.context, ti, structlog.get_logger())
    if state != "success" or ti.get_dr_count(dag_id=DUMMY, run_ids=[run_id]) != 1:
        raise AirflowFailException(f"ран {DUMMY}/{run_id} не создан (state={state})")


# ─────────────────────────────── Тесты: стандартные — HTTP ────────────────────────
HTTP_CONN = "af_api"  # свой API контура; заводит ядро


def _http(t: T, endpoint: str):
    from airflow.sdk import BaseHook
    try:
        BaseHook.get_connection(HTTP_CONN)
    except Exception:
        raise AirflowSkipException(f"нет подключения {HTTP_CONN}") from None
    import airflow
    result, _ = t.run(http_conn_id=HTTP_CONN, method="GET", endpoint=endpoint, response_filter=lambda r: r.json())
    if (result or {}).get("version") != airflow.__version__:
        raise AirflowFailException(f"ответ {result}, ждём версию {airflow.__version__}")


def http_version(t: T):
    """HttpOperator: GET api/v2/version своего API, версия = версии рана"""
    _http(t, "api/v2/version")


def http_tpl(t: T):
    """HttpOperator шаблоном: endpoint из {{ }}"""
    _http(t, "{{ 'api/v2/version' }}")


OPS = [
    # шаблонного случая у PG→PG нет: у оператора пустые template_fields
    Op("pg_to_pg", "postgres_to_postgres", "HrpPostgresToPostgresOperator", {"pg"},
       [pg_to_pg_full, pg_to_pg_chunked]),
    Op("pg_inc", "postgres_to_postgres", "HrpPostgresIncarnationInsertOperator", {"pg"},
       [pg_inc_full, pg_inc_chunked, pg_inc_tpl]),
    Op("pg_ddl", "postgres_ddl", "HrpPostgresDDL", {"pg"}, [pg_ddl_table, pg_ddl_tpl]),
    Op("pg_to_s3", "postgres_to_s3", "HrpPostgresToS3Operator", {"pg", "s3"},
       [pg_to_s3_gzip, pg_to_s3_none, pg_to_s3_tpl]),
    Op("pg_to_s3_list", "postgres_to_s3", "HrpPostgresToS3ListOperator", {"pg", "s3"},
       [pg_to_s3_list_gzip, pg_to_s3_list_zip, pg_to_s3_list_targz, pg_to_s3_list_none, pg_to_s3_list_sanitize,
        pg_to_s3_list_tpl]),
    Op("s3_to_pg", "s3_to_postgres", "HrpS3ToPostgresOperator2", {"pg", "s3"}, [s3_to_pg_gzip, s3_to_pg_tpl]),
    # плагин af_dags, не ядро: на сигме его нет — группа «не создана»
    Op("s3_to_pg_stream", "s3_to_pg_stream", "HrpS3ToPgStreamOperator", {"pg", "s3"},
       [s3_to_pg_stream_csv, s3_to_pg_stream_json, s3_to_pg_stream_tpl]),
    Op("s3_to_s3", "s3_to_s3", "HrpS3ToS3Operator", {"s3"}, [s3_to_s3_gzip, s3_to_s3_zip, s3_to_s3_tpl]),
    Op("ch_native_to_s3", "clickhouse_to_s3", "HrpClickNativeToS3Operator", {"ch", "s3"},
       [ch_native_to_s3_gzip, ch_native_to_s3_zip, ch_native_to_s3_targz, ch_native_to_s3_none,
        ch_native_to_s3_sanitize, ch_native_to_s3_tpl]),
    Op("ch_native_list", "clickhouse_to_s3", "HrpClickNativeToS3ListOperator", {"ch", "s3"},
       [ch_native_list_gzip, ch_native_list_zip, ch_native_list_targz, ch_native_list_none, ch_native_list_tpl]),
    Op("ch_table_to_s3", "clickhouse_to_s3", "HrpClickhouseTableToS3Operator", {"ch", "s3", "ch_http", "cluster"},
       [ch_table_to_s3_gzip, ch_table_to_s3_none, ch_table_to_s3_tpl]),
    Op("ch_query_to_s3", "clickhouse_to_s3", "HrpClickhouseQueryToS3Operator", {"ch", "s3", "ch_http", "cluster"},
       [ch_query_to_s3_gzip, ch_query_to_s3_tpl]),
    Op("ch_to_pg", "clickhouse_to_postgres", "HrpClickhouseToPostgresOperator", {"ch", "pg"},
       [ch_to_pg_full, ch_to_pg_tpl]),
    Op("ch_to_pg_inc", "clickhouse_to_postgres", "HrpClickhouseToPostgresIncarnationOperator", {"ch", "pg"},
       [ch_to_pg_inc_full, ch_to_pg_inc_tpl]),
    # шаблонного случая нет: у оператора пустые template_fields
    Op("pg_to_ch", "postgres_to_clickhouse", "HrpPostgresToClickhouseOperator", {"pg", "ch", "ch_http"},
       [pg_to_ch_table]),
    Op("ch_to_ch", "click_to_click", "HrpClickhouseToClickhouseOperator", {"ch", "ch_http"},
       [ch_to_ch_stream, ch_to_ch_typed, ch_to_ch_tpl]),
    Op("ch_cluster", "clickhouse_cluster_operator", "HrpClickHouseClusterOperator", {"ch", "cluster"},
       [ch_cluster_ddl, ch_cluster_tpl]),
    Op("s3_to_ch_table", "s3_to_clickhouse", "HrpS3ToClickhouseTableOperator", {"s3", "ch", "ch_http"},
       [s3_to_ch_table_csv, s3_to_ch_table_tsv, s3_to_ch_table_tpl]),
    Op("s3_to_ch_transformed", "s3_to_clickhouse", "HrpS3ToClickhouseTransformedOperator", {"s3", "ch", "ch_http"},
       [s3_to_ch_transformed_tsv, s3_to_ch_transformed_tpl]),
    Op("s3_archive", "s3_archive", "HrpS3ArchiveOperator", {"s3"}, [s3_archive_two, s3_archive_tpl]),
    Op("s3_file_hash", "s3_file_hash", "HrpCheckS3FileHash", {"s3"},
       [s3_file_hash_ok, s3_file_hash_mismatch, s3_file_hash_tpl]),
    Op("s3_list_keys", "s3_viewer_operator", "HrpS3ListKeysOperator", {"s3"}, [s3_list_keys_all, s3_list_keys_tpl]),
    Op("s3_file_read", "s3_viewer_operator", "HrpS3FileReadOperator", {"s3"}, [s3_file_read_rows, s3_file_read_tpl]),
    Op("s3_bucket_viewer", "s3_viewer_operator", "HrpS3BucketViewerOperator", {"s3"},
       [s3_bucket_viewer_list, s3_bucket_viewer_tpl]),
    Op("lineage", "utils.lineage", "extract_lineage_from_sql", set(), [lineage_sql], policy=False),
]
STD = "airflow.providers"  # стандартные операторы: классы из пакетов, которые импортируют даги контура
STD_OPS = [
    Op("empty", "standard.operators.empty", "EmptyOperator", set(), [empty_run], pkg=STD),
    Op("python", "standard.operators.python", "PythonOperator", set(), [python_args, python_tpl], pkg=STD),
    Op("task", "standard.decorators.python", "_PythonDecoratedOperator", set(), [task_args, task_tpl], pkg=STD),
    Op("branch_python", "standard.operators.python", "BranchPythonOperator", set(),
       [branch_python_both, branch_python_tpl], pkg=STD),
    Op("short_circuit", "standard.operators.python", "ShortCircuitOperator", set(),
       [short_circuit_both, short_circuit_tpl], pkg=STD),
    Op("task_sensor", "standard.decorators.sensor", "DecoratedSensorOperator", set(),
       [task_sensor_ok, task_sensor_timeout, task_sensor_tpl], pkg=STD),
    Op("sql_execute", "common.sql.operators.sql", "SQLExecuteQueryOperator", {"pg"},
       [sql_execute_select, sql_execute_tpl], pkg=STD),
    Op("ch_operator", "operators.clickhouse", "ClickHouseOperator", {"ch"},
       [ch_operator_rows, ch_operator_tpl], pkg="airflow_clickhouse_plugin"),
    Op("ch_branch_sql", "operators.clickhouse_dbapi", "ClickHouseBranchSQLOperator", {"ch"},
       [ch_branch_sql_both, ch_branch_sql_tpl], pkg="airflow_clickhouse_plugin"),
    Op("ch_sensor", "sensors.clickhouse", "ClickHouseSensor", {"ch"},
       [ch_sensor_ok, ch_sensor_timeout], pkg="airflow_clickhouse_plugin"),
    Op("s3_delete", "amazon.aws.operators.s3", "S3DeleteObjectsOperator", {"s3"},
       [s3_delete_keys, s3_delete_prefix, s3_delete_tpl], pkg=STD),
    Op("s3_key_sensor", "amazon.aws.sensors.s3", "S3KeySensor", {"s3"},
       [s3_key_sensor_ok, s3_key_sensor_timeout, s3_key_sensor_tpl], pkg=STD),
    Op("s3_create_bucket", "amazon.aws.operators.s3", "S3CreateBucketOperator", {"s3"},
       [s3_create_bucket_exists, s3_create_bucket_tpl], pkg=STD),
    Op("trigger_dagrun", "standard.operators.trigger_dagrun", "TriggerDagRunOperator", set(),
       [trigger_dagrun_conf], pkg=STD),
    # замер сигмы dev за всё время (22.10)
    Op("task_branch", "standard.decorators.branch_python", "_BranchPythonDecoratedOperator", set(),
       [task_branch_both, task_branch_tpl], pkg=STD),
    Op("branch_sql", "common.sql.operators.sql", "BranchSQLOperator", {"pg"}, [branch_sql_both, branch_sql_tpl], pkg=STD),
    Op("ch_sql_execute", "operators.clickhouse_dbapi", "ClickHouseSQLExecuteQueryOperator", {"ch"},
       [ch_sql_execute_select, ch_sql_execute_tpl], pkg="airflow_clickhouse_plugin"),
    Op("s3_copy", "amazon.aws.operators.s3", "S3CopyObjectOperator", {"s3"}, [s3_copy_key, s3_copy_tpl], pkg=STD),
    Op("s3_create_object", "amazon.aws.operators.s3", "S3CreateObjectOperator", {"s3"},
       [s3_create_object_data, s3_create_object_tpl], pkg=STD),
    Op("http", "http.operators.http", "HttpOperator", set(), [http_version, http_tpl], pkg=STD),
]
NOT_CREATED: dict[str, str] = {}  # группа → причина; заполняет разбор, читает report
# главные группы: Hrp-операторы и стандартные; task_id = <главная>.<группа>.<тест>
TOPS = ("tests.", "test_standart.")
# в verdict группа — без главной группы: имена обязаны не совпадать
assert not {o.group for o in OPS} & {o.group for o in STD_OPS}, "имя группы в tests и test_standart"


# ───────────────────────────────── systems ────────────────────────────────────
def _facts(conn_id: str, version: str = "") -> str:
    """Тип и версия из conn_facts (пишет tools_test_connections); нет записи — из version() пробы."""
    from airflow.sdk import Variable
    try:
        f = (Variable.get("conn_facts", default=None, deserialize_json=True) or {}).get(conn_id) or {}
    except Exception:
        logger.warning("Variable conn_facts не прочиталась", exc_info=True)
        f = {}
    if not f and version:
        m = re.search(r"Greenplum Database (\S+)", version) or re.search(r"PostgreSQL (\S+)", version)
        f = {"kind": "GP" if "Greenplum" in version else "PG", "version": m and m.group(1).rstrip(",)")}
    return " ".join(str(x) for x in (f.get("kind"), f.get("version")) if x)


def _probe_pg(conn_id: str, schema: str) -> dict:
    try:
        with closing(_REAL_GET_CONN(PostgresHook(postgres_conn_id=conn_id))) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                if not schema:
                    cur.execute(f"CREATE TEMP TABLE {PROBE} (x int)")
                    return {"ok": True, "ddl": True, "version": version, "reason": ""}
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {schema}.{PROBE}")
                    cur.execute(f"CREATE TABLE {schema}.{PROBE} (x int)")
                    cur.execute(f"DROP TABLE {schema}.{PROBE}")
                except Exception as e:
                    return {"ok": True, "ddl": False, "version": version,
                            "reason": f"нет DROP (CREATE) на {conn_id}.{schema}: {e}"[:300]}
                return {"ok": True, "ddl": True, "version": version, "reason": ""}
    except Exception as e:
        return {"ok": False, "reason": f"{conn_id}: {type(e).__name__}: {e}"[:300]}


def _probe_ch(conn_id: str, db: str) -> dict:
    from airflow.sdk import Variable
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    try:
        ch = ClickHouseHook(clickhouse_conn_id=conn_id)
        ch.execute(f"DROP TABLE IF EXISTS {db}.{PROBE}")
        ch.execute(f"CREATE TABLE {db}.{PROBE} (x Int32) ENGINE = MergeTree() ORDER BY x")
        ch.execute(f"DROP TABLE {db}.{PROBE}")
    except Exception as e:
        return {"ok": False, "reason": f"{conn_id}.{db}: {type(e).__name__}: {e}"[:300]}
    # кластер — из ch_clusters (пишет tools_test_connections), сами system.clusters не читаем
    try:
        clusters = Variable.get("ch_clusters", default=None, deserialize_json=True)
    except Exception:
        logger.warning("Variable ch_clusters не прочиталась", exc_info=True)
        clusters = None
    entry = next((e for e in (clusters or {}).values() if conn_id in (e.get("conns") or [])), None)
    if entry is None:
        cluster = {"ok": False, "reason": "нет сведений о кластере — запустите tools_test_connections"}
    elif "datalab" not in (entry.get("clusters") or {}):
        cluster = {"ok": False, "reason": f"у {conn_id} нет кластера datalab"}
    else:
        from airflow.sdk import BaseHook
        nodes = entry["clusters"]["datalab"]
        lost = []
        for n in nodes:  # оператор ходит на узел через click-dlab-<короткое имя>
            try:
                BaseHook.get_connection(f"click-dlab-{n.split('.')[0]}")
            except Exception:
                lost.append(f"click-dlab-{n.split('.')[0]}")
        cluster = ({"ok": False, "reason": f"нет подключений {', '.join(lost)}"} if lost
                   else {"ok": True, "reason": "", "nodes": nodes})
    return {"ok": True, "reason": "", "cluster": cluster}


def _probe_s3(conn_id: str, bucket: str, folder: str) -> dict:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    try:
        s3 = S3Hook(aws_conn_id=conn_id)
        if not s3.check_for_bucket(bucket):
            return {"ok": False, "reason": f"нет бакета {bucket} ({conn_id})"}
        key = f"{folder.strip('/')}/{PROBE}"
        s3.load_string("probe", key=key, bucket_name=bucket, replace=True)
        s3.delete_objects(bucket=bucket, keys=[key])
    except Exception as e:
        return {"ok": False, "reason": f"{conn_id}://{bucket}: {type(e).__name__}: {e}"[:300]}
    return {"ok": True, "reason": ""}


# ──────────────────────────────── DAG ─────────────────────────────────────────
_S3_DEFAULT = log_bucket_path("test-operators") or "s3-archive://dataplatform-monitoring/test-operators"


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
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        # общие CH и S3 рвут соединение («Connection reset by peer»): без повтора одно моргание сети
        # краснит гейт релиза; повтор безопасен — подготовка сначала убирает остатки
        "retries": 2,
        "retry_delay": dt.timedelta(seconds=30),
        # Тестовые таблицы — три строки: потолок ловит зависание, а не объём
        "execution_timeout": dt.timedelta(minutes=30),
        "on_success_callback": on_callback,
        "on_failure_callback": on_callback,
        "on_skipped_callback": on_callback,
    },
    params={
        "pg": Param(SAVED.get("pg", "airflowdb.hrp_test"), type="string", pattern=PG_PATTERN,
                    description="Postgres/GP: conn_id.схема — таблицы в схеме (нужны CREATE и DROP); "
                                "только conn_id — временные таблицы сессии"),
        "ch": Param(SAVED.get("ch", "dlab-click.technical"), type="string", pattern=DB_PATTERN,
                    description="ClickHouse для тестовых таблиц: conn_id.база"),
        "s3": Param(SAVED.get("s3", _S3_DEFAULT), type="string", pattern=S3_PATTERN,
                    description="Тестовые файлы: conn_id://бакет/папка; каждый тест чистит свою подпапку"),
        "test_pg": Param(SAVED.get("test_pg", True), type="boolean", description="Тесты с Postgres/GP"),
        "test_ch": Param(SAVED.get("test_ch", True), type="boolean", description="Тесты с ClickHouse"),
        "test_s3": Param(SAVED.get("test_s3", True), type="boolean", description="Тесты с S3"),
        # HTTP-клиент ядра берёт из подключения только хост — порт 8123 без TLS; на альфе DEV HTTP
        # у ClickHouse закрыт (Connection reset by peer, 06.10.2026)
        "ch_http": Param(SAVED.get("ch_http", SAVED.get("run_ch_http", True)), type="boolean",
                         description="Тесты операторов, которые ходят в ClickHouse по HTTP"),
        "run_cleanup": Param(SAVED.get("run_cleanup", True), type="boolean",
                             description="Убрать за тестами таблицы и ключи S3; выкл. — оставить для разбора"),
        "save_params": Param(False, type="boolean",
                             description=f"Сохранить форму в Variable {PARAMS_VAR} как значения по умолчанию"),
        **{f"op_{op.group}": Param(SAVED.get(f"op_{op.group}", True), type="boolean", section="Операторы",
                                   description=op.cls) for op in OPS},
        **{f"op_{op.group}": Param(SAVED.get(f"op_{op.group}", True), type="boolean",
                                   section="Стандартные операторы", description=op.cls) for op in STD_OPS},
        "op_skip_graph": Param(SAVED.get("op_skip_graph", True), type="boolean", section="Стандартные операторы",
                               description="пропуск веток в графе"),
    },
    doc_md=__doc__,
    description="Регресс операторов hrp_operators и стандартных по группам: pg, ch, s3",
)
def test_hrp_operators_dag():

    @task(task_id="systems", retries=0, trigger_rule=TriggerRule.ALL_DONE)
    def systems(**context):
        """Системы: доступность включённых систем и права на объекты; тесты сами решают уйти в ☮️"""
        status, msg = store_params(PARAMS_VAR, SAVED, context)
        if status == "ok":
            add_note(msg, context, level="task")
        p = context["params"]
        res, line = {}, []
        if p["test_pg"]:
            conn, _, schema = p["pg"].partition(".")
            res["pg"] = r = _probe_pg(conn, schema)
            mode = f"схема {schema}" if schema else "временные таблицы"
            line.append(f"{_facts(conn, r.get('version', '')) or 'PG'} ({mode}) "
                        + ("✅" if r["ok"] and r.get("ddl") else f"❌ {r['reason']}"))
        if p["test_ch"]:
            conn, _, db = p["ch"].partition(".")
            res["ch"] = r = _probe_ch(conn, db)
            cl = r.get("cluster") or {}
            line.append(f"{_facts(conn) or 'CH'} " + ("✅" if r["ok"] else f"❌ {r['reason']}")
                        + (" · кластер datalab " + ("✅" if cl.get("ok") else f"☮️ {cl.get('reason')}")
                           if r["ok"] else ""))
        if p["test_s3"]:
            conn, _, rest = p["s3"].partition("://")
            bucket, _, folder = rest.partition("/")
            res["s3"] = r = _probe_s3(conn, bucket, folder)
            line.append("S3 " + ("✅" if r["ok"] else f"❌ {r['reason']}"))
        add_note("Системы: " + (" · ".join(line) or "все выключены"), context, level="DAG")
        if status == "fail":
            raise AirflowFailException(msg)
        return res

    @task(task_id="import", retries=0, trigger_rule=TriggerRule.ALL_DONE)
    def broken_import(group: str, why: str, **context):
        """Импорт оператора сломан: группа не может выполнить тесты"""
        if not context["params"].get(f"op_{group}", True):
            raise AirflowSkipException(f"{group}: выключен (op_{group}=False)")
        raise AirflowFailException(why)

    def make_test(op: Op, case):
        name = case.__name__.removeprefix(f"{op.group}_")

        @task(task_id=name, trigger_rule=TriggerRule.ALL_DONE, doc_md=case.__doc__)
        def _test(**context):
            t = T(context, op, name)
            try:
                t.gate()
            except BaseException:
                t.close()  # ☮️/❌ до подготовки: убирать нечего, а система может быть недоступна
                raise
            try:
                t.clean_leftovers()
                case(t)
            finally:
                # уборка и при ❌, и при execution_timeout: finally выполняется на AirflowTaskTimeout
                if context["params"]["run_cleanup"]:
                    t.cleanup()
                if t.not_cleaned:
                    context["ti"].xcom_push(key="not_cleaned", value=t.not_cleaned)
                t.close()

        return _test()

    @task(task_id="check", trigger_rule=TriggerRule.ALL_DONE, retries=0,
          doc_md="Пропуск веток в графе: Branch и ShortCircuit настоящими задачами, невыбранные ☮️")
    def skip_graph_check(**context):
        if not context["params"].get("op_skip_graph", True):
            raise AirflowSkipException("skip_graph: выключен (op_skip_graph=False)")
        ti, dr = context["ti"], context["dag_run"]
        states = ti.get_task_states(dag_id=dr.dag_id, run_ids=[dr.run_id]).get(dr.run_id, {})
        got = {x: states.get(SKIP_GRAPH + x) for x in GRAPH_WANT}
        if got != GRAPH_WANT:
            raise AirflowFailException(f"состояния {got}, ждём {GRAPH_WANT}")

    # retries=0: провал вердикта — итог, а не сбой, перезапуск ничего не изменит
    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def report(**context):
        """Вердикт: сводка по группам в заметке рана, XCom `verdict`; есть ❌ или недоступная система — падает"""
        ti, dag_run, dag_ = context["ti"], context["dag_run"], context["dag"]
        # Airflow 3: у задачи нет сессии метабазы — состояния соседей через Execution API
        states = ti.get_task_states(dag_id=dag_run.dag_id, run_ids=[dag_run.run_id]).get(dag_run.run_id, {})
        groups: dict = {}
        failed = []
        # по задачам дага, а не по ответу API: задача без строки в метабазе — ❌, а не пропажа из сводки
        # без systems, report и служебных задач skip_graph
        test_ids = sorted(tid for tid in dag_.task_ids if tid.startswith(TOPS) and tid not in GRAPH_TASKS)
        full = {}  # <группа>.<тест> → task_id: в verdict без главной группы, как до группы tests
        for task_id in test_ids:
            state = states.get(task_id) or "no_status"
            icon = "✅" if state == "success" else "☮️" if state in ("skipped", "upstream_failed", "removed") else "❌"
            _, g, case = task_id.split(".", 2)
            full[f"{g}.{case}"] = task_id
            groups.setdefault(g, {})[case] = icon
            if icon == "❌":
                failed.append(f"{g}.{case}")
        pulled = ti.xcom_pull(task_ids=test_ids, key="not_cleaned") if test_ids else []
        if pulled and not isinstance(pulled, list):
            pulled = [pulled]
        # у каждого теста — список строк; одиночную строку по символам не разбираем
        not_cleaned = [x for v in pulled or [] if v for x in ([v] if isinstance(v, str) else v)]

        sys_res = ti.xcom_pull(task_ids="systems") or {}
        p = context["params"]
        unavailable = [s.upper() for s, flag in (("pg", "test_pg"), ("ch", "test_ch"), ("s3", "test_s3"))
                       if p.get(flag) and not (sys_res.get(s) or {}).get("ok")]

        icons = [i for cases in groups.values() for i in cases.values()]
        ok, skip = icons.count("✅"), icons.count("☮️")
        passed = not (failed or unavailable)
        headline = (f"🧪 Регресс операторов {'✅ пройден' if passed else '❌ провален'}: "
                    f"✅ {ok} / ❌ {len(failed)} / ☮️ {skip}")
        lines = [f"❌ {tid} — {(dag_.get_task(full[tid]).doc_md or '').split(':')[0]}" for tid in failed]
        std = {o.group for o in STD_OPS} | {SKIP_GRAPH.split(".")[1]}
        lines += [f"{g}: " + " ".join(f"{i}{c}" for c, i in cases.items())
                  for g, cases in sorted(groups.items()) if g not in std]
        if std & groups.keys():
            lines.append("Стандартные операторы:")
            lines += [f"{g}: " + " ".join(f"{i}{c}" for c, i in cases.items())
                      for g, cases in sorted(groups.items()) if g in std]
        if unavailable:
            lines.append("недоступны: " + ", ".join(unavailable))
        missing = sorted(g for g, r in NOT_CREATED.items() if r != "запрещён")
        forbidden = sorted(g for g, r in NOT_CREATED.items() if r == "запрещён")
        if missing:
            lines.append("не созданы: нет в пакете: " + ", ".join(missing))
        if forbidden:
            lines.append("не созданы: запрещён: " + ", ".join(forbidden))
        if not_cleaned:
            lines.append("не убрано: " + "; ".join(not_cleaned))
        add_note("\n".join(lines), context, level="DAG", title=headline)
        logger.info("%s\n%s", headline, "\n".join(lines))

        # XCom — до исключения, иначе DPM не узнает, что именно упало. Версию — через core_version:
        # на сигме пакет собран pyinstaller'ом без метаданных, importlib.metadata.version там падает
        from sber_app_dataplatform_etl_core.build_info import core_version
        ti.xcom_push(key="verdict", value={
            "passed": passed, "ok": ok, "failed": failed, "skipped": skip,
            "unavailable": unavailable, "core": core_version(), "groups": groups,
            "not_created": {g: ("запрещён" if r == "запрещён" else "нет в пакете") for g, r in NOT_CREATED.items()},
        })
        if not passed:
            raise AirflowFailException(
                "регресс операторов провален: "
                + "; ".join(filter(None, [f"упали {', '.join(failed)}" if failed else "",
                                          f"недоступны {', '.join(unavailable)}" if unavailable else ""])))

    systems_t = systems()
    leaves = [systems_t]
    NOT_CREATED.clear()  # процесс разбора живёт долго: без этого остаются группы прошлых разборов
    for top, ops, tip in (("tests", OPS, "Тесты операторов: группа на оператор"),
                          ("test_standart", STD_OPS, "Стандартные операторы Airflow и провайдеров")):
        with TaskGroup(top, tooltip=tip):
            for op in ops:
                op.loaded, status, why = _load(op)
                if status in ("missing", "forbidden"):
                    NOT_CREATED[op.group] = "запрещён" if status == "forbidden" else why
                    continue
                with TaskGroup(op.group, tooltip=op.cls):
                    if status == "broken":
                        imp = broken_import(op.group, why)
                        systems_t >> imp
                        leaves.append(imp)
                        continue
                    prev = systems_t
                    for case in op.cases:
                        cur = make_test(op, case)
                        prev >> cur
                        prev = cur
                    leaves.append(prev)  # ALL_DONE по цепочке: последний тест группы выполняется после всех
            # нет Branch или ShortCircuit — подгруппы нет молча: причина уже в строке их группы
            if ops is STD_OPS and all(o.loaded for o in STD_OPS if o.group in ("branch_python", "short_circuit")):
                with TaskGroup("skip_graph", tooltip="Пропуск веток настоящими задачами графа"):
                    from airflow.providers.standard.operators.empty import EmptyOperator
                    by = {o.group: o.loaded for o in STD_OPS}
                    br = by["branch_python"](task_id="branch", python_callable=_graph_branch)
                    # False: пропустить только прямого потомка, иначе ☮️ уйдут check и report
                    sc = by["short_circuit"](task_id="short", python_callable=lambda: False,
                                             ignore_downstream_trigger_rules=False)
                    keep, drop, after = (EmptyOperator(task_id=x) for x in ("keep", "drop", "after"))
                    chk = skip_graph_check()
                    systems_t >> [br, sc]
                    br >> [keep, drop]
                    sc >> after
                    [keep, drop, after] >> chk  # check — ALL_DONE: выполняется и после ☮️ веток
                    leaves.append(chk)
    # report — единственный лист: ран краснеет через него
    leaves >> report()


test_hrp_operators_dag()
