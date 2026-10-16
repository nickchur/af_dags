"""### 🧪 DAG: Регрессионный стенд операторов HRP
*2026-10-16 17:52 MSK · v2.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Регресс операторов `hrp_operators` на каждый релиз. Каждый оператор — своя группа задач, каждый
тест — одна задача: сам готовит вход (таблицу, файл в S3), выполняет оператор как в бою (Jinja
окружением дага, политика контура) и сверяет результат, после себя убирает. Тесты группы идут
друг за другом, группы — параллельно. Оператора нет в пакете или он запрещён политикой — группы
нет; сломан импорт — в группе одна красная задача. Первая задача `systems` проверяет системы и
права, последняя `report` пишет сводку по группам. Запуск вручную (`@once`).

PG: `conn.схема` — таблицы в схеме (нужны CREATE и DROP), только `conn` — временные таблицы
сессии, в базе после рана ничего не остаётся. GP — так же, как PG.

Вердикт (гейт релиза в DPM): ран `failed`, если есть ❌ или включённая система недоступна;
детали — XCom `verdict` таска `report`.

Подробно: [tools/readme.md — test_hrp_operators](../../_plugin_dag_docs/?doc=tools/readme.md#test_hrp_operatorspytest_hrp_operatorspy)
"""

import csv
import datetime as dt
import gzip
import importlib
import io
import os
import re
import sys
import tempfile
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
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException  # noqa: E402

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
EXPECTED_ROWS = len(ROWS)


def _pg_create_sql(table: str, temp: bool, incarnation: bool = False, inc_col: bool = True) -> list[str]:
    """CREATE (TEMP) TABLE источника/цели; incarnation — _0/_1 + последовательность _inc_seq.

    table — с схемой в режиме схемы, без схемы во временном. Комментарии — только у физических
    таблиц (Quality Gate). DROP здесь нет: остатки убирает подготовка теста по префиксу.
    """
    t = "TEMP " if temp else ""
    cols = (["incarnation integer"] if incarnation and inc_col else []) + [
        f"{name} {pg_type}{' PRIMARY KEY' if name == 'id' and not incarnation else ''}"
        for name, pg_type, *_ in COLUMNS]
    tables = [f"{table}_0", f"{table}_1"] if incarnation else [table]
    stmts = [f"CREATE {t}SEQUENCE {table}_inc_seq START WITH 0 MINVALUE 0"] if incarnation else []
    for tbl in tables:
        stmts.append(f"CREATE {t}TABLE {tbl} ({', '.join(cols)})")
        if not temp:
            stmts.append(f"COMMENT ON TABLE {tbl} IS 'HRP regression'")
            stmts += [f"COMMENT ON COLUMN {tbl}.{name} IS '{comment}'" for name, *_, comment in COLUMNS]
    return stmts


def _csv_bytes(rows=ROWS) -> bytes:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows([COL_NAMES, *rows])
    return buf.getvalue().encode()


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
    if allowed is not None and op.cls not in allowed:
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
        self._ops = 0
        self._tmp = tempfile.TemporaryDirectory(prefix=self.prefix, ignore_cleanup_errors=True)
        self.tmp = self._tmp.name
        self.not_cleaned: list[str] = []

    # ── решение «идти или ☮️» ──
    def gate(self):
        p = self.context["params"]
        if not p.get(f"op_{self.op.group}", True):
            raise AirflowSkipException(f"{self.op.group}: выключен (op_{self.op.group}=False)")
        systems = self.context["ti"].xcom_pull(task_ids="systems")
        if systems is None:  # {} — все системы выключены, это не сбой
            raise AirflowFailException("нет результата задачи systems")
        for s in sorted(self.op.systems):
            flag = {"pg": "test_pg", "ch": "test_ch", "s3": "test_s3", "ch_http": "ch_http"}.get(s)
            if flag and not p.get(flag):
                raise AirflowSkipException(f"{flag}=False")
            st = systems.get(s)
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
        spy_cm = mock.patch.object(op, spy, wraps=getattr(op, spy)) if spy else nullcontext()
        with self.pg_sessions(sessions), spy_cm as m:
            result = op.execute(ctx)
        return result, m

    def expect_rows(self, result, actual: int):
        """REQ-19: в цели EXPECTED_ROWS строк, а row_count оператора (если есть) равен числу в цели."""
        rc = result.get("row_count") if isinstance(result, dict) else None
        if actual != EXPECTED_ROWS or (rc is not None and int(rc) != actual):
            raise AirflowFailException(f"в цели {actual} строк (ждём {EXPECTED_ROWS}), row_count={rc}")

    # ── уборка ──
    def _drop_own(self):
        """Удаляет объекты теста: таблицы и последовательности по префиксу, ключи под своей папкой S3."""
        if "pg" in self.op.systems and not self.temp:
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
        if "s3" in self.op.systems:
            keys = self.s3.list_keys(bucket_name=self.bucket, prefix=self.s3_prefix)
            if keys:
                # у S3Hook delete_objects принимает bucket, а не bucket_name
                self.s3.delete_objects(bucket=self.bucket, keys=keys)

    def clean_leftovers(self):
        """Остатки убитого прогона: DROP … IF EXISTS по префиксу и очистка своей папки до подготовки."""
        self._drop_own()

    def cleanup(self):
        try:
            self._drop_own()
        except Exception as e:  # исход теста уборка не подменяет (REQ-23)
            logger.exception("уборка %s не удалась", self.prefix)
            self.not_cleaned.append(f"{self.prefix}: {type(e).__name__}: {e}"[:300])
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


OPS = [
    # шаблонного случая у PG→PG нет: у оператора пустые template_fields
    Op("pg_to_pg", "postgres_to_postgres", "HrpPostgresToPostgresOperator", {"pg"},
       [pg_to_pg_full, pg_to_pg_chunked]),
    Op("s3_to_s3", "s3_to_s3", "HrpS3ToS3Operator", {"s3"}, [s3_to_s3_gzip, s3_to_s3_zip, s3_to_s3_tpl]),
]
NOT_CREATED: dict[str, str] = {}  # группа → причина; заполняет разбор, читает report


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
        cluster = {"ok": True, "reason": "", "nodes": entry["clusters"]["datalab"]}
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
    },
    doc_md=__doc__,
    description="Регресс операторов hrp_operators по группам: pg, ch, s3",
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
        test_ids = sorted(tid for tid in dag_.task_ids if "." in tid)  # без systems и report
        for task_id in test_ids:
            state = states.get(task_id) or "no_status"
            icon = "✅" if state == "success" else "☮️" if state in ("skipped", "upstream_failed", "removed") else "❌"
            g, case = task_id.split(".", 1)
            groups.setdefault(g, {})[case] = icon
            if icon == "❌":
                failed.append(task_id)
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
        headline = (f"🧪 HRP operators {'✅ пройден' if passed else '❌ провален'}: "
                    f"✅ {ok} / ❌ {len(failed)} / ☮️ {skip}")
        lines = [f"❌ {tid} — {(dag_.get_task(tid).doc_md or '').split(':')[0]}" for tid in failed]
        lines += [f"{g}: " + " ".join(f"{i}{c}" for c, i in cases.items()) for g, cases in sorted(groups.items())]
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
    for op in OPS:
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
    # report — единственный лист: ран краснеет через него
    leaves >> report()


test_hrp_operators_dag()
