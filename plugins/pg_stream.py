"""###🛠️ Потоковая загрузка S3 → GP/PG (`plugins/pg_stream.py`)
*2026-10-15 13:04 MSK · v0.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

`HrpS3ToPgStreamOperator` — один объект S3 в одну таблицу Greenplum или PostgreSQL:
потоково, одной транзакцией, одной командой COPY на файл. Усечение приёмника —
`truncate` = None | soft | hard | auto. Итог (строки источника и загруженные, типы
колонок приёмника) — в XCom `return_value`.

Сейчас — CSV без сжатия; JSON, раскладка по колонкам, gz/zip и `file_name_column` — дальше.
Самопроверка: `python3 plugins/pg_stream.py`.
"""
from __future__ import annotations

import csv
import io
import os
import re
from contextlib import closing

import psycopg2
import psycopg2.errors
from psycopg2 import sql

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

try:
    from plugins.s3_utils import s3_IterStream  # type: ignore
except ImportError:
    from CI06932748.tools.s3_utils import s3_IterStream  # type: ignore

TRUNCATE_MODES = (None, "soft", "hard", "auto")
FORMATS = ("auto", "csv", "json", "jsonl")
CHUNK = 1 << 20     # байт за одно чтение из S3 и за одно чтение COPY

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$")
_EXT = {".csv": "csv", ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl"}
_GP_REJECTED = re.compile(r"found (\d+) data formatting errors")
_REJECT_LIMIT = re.compile(r"\bREJECT\s+LIMIT\b", re.I)
_OPT_CHAR = r"\b{}\s+(?:AS\s+)?E?'((?:\\.|[^'])+)'"


def parse_table(table):
    """`схема.таблица` или `таблица` → безопасный идентификатор; иное — ValueError."""
    if not _IDENT.match(table or ""):
        raise ValueError(f"недопустимое имя таблицы: {table!r}")
    return sql.SQL(".").join(sql.Identifier(p) for p in table.split("."))


def detect_format(name, fmt):
    """Формат файла: заданный явно или по расширению имени (`auto`)."""
    if fmt != "auto":
        if fmt not in FORMATS:
            raise ValueError(f"format={fmt!r}: ждём csv, json, jsonl или auto")
        return fmt
    ext = os.path.splitext(name.lower())[1]
    if ext not in _EXT:
        raise ValueError(f"формат не определён по имени {name}: задайте format")
    return _EXT[ext]


def _truncate(cur, tbl, mode):
    """Усечение приёмника в текущей транзакции; возвращает способ: truncate | delete | None."""
    if mode is None:
        return None
    if mode == "hard":
        cur.execute(sql.SQL("truncate {}").format(tbl))
        return "truncate"
    if mode == "soft":
        cur.execute(sql.SQL("delete from {}").format(tbl))
        return "delete"
    # auto — как srv_wf.pr_soft_truncate, но без функций схемы: их нет на PG и у читающих
    # подключений. TRUNCATE держит ACCESS EXCLUSIVE до конца загрузки, поэтому берём его,
    # только если таблица свободна сейчас; занята — DELETE, читатели не ждут.
    cur.execute("savepoint pg_stream_trunc")
    try:
        cur.execute(sql.SQL("lock table {} in access exclusive mode nowait").format(tbl))
        cur.execute(sql.SQL("truncate {}").format(tbl))
        cur.execute("release savepoint pg_stream_trunc")
        return "truncate"
    except psycopg2.errors.LockNotAvailable:
        cur.execute("rollback to savepoint pg_stream_trunc")
        cur.execute(sql.SQL("delete from {}").format(tbl))
        return "delete"


def _target_types(cur, description):
    """Колонки приёмника с короткими именами типов (`int4`, `varchar`) — как srv_wf.pr_get_format_type."""
    cur.execute("select oid, typname::text from pg_catalog.pg_type where oid = any(%s)",
                ([c.type_code for c in description],))
    names = dict(cur.fetchall())
    return {c.name: names.get(c.type_code, str(c.type_code)) for c in description}


def _opt_char(options, name, default):
    m = re.search(_OPT_CHAR.format(name), options, re.I)
    if not m:
        return default
    return "\t" if m.group(1) == "\\t" else m.group(1)


class CsvCounter:
    """Считает записи CSV в потоке байт, который идёт в COPY без изменений.

    Запись кончается переводом строки вне кавычек: чётность кавычек считается по байтам
    (кавычка — ASCII, в многобайтных символах UTF-8 и cp1251 не встречается), поэтому без
    декодирования и посимвольного цикла. Перевод строки внутри кавычек записи не делит —
    иначе rows_source разошёлся бы с rowcount и дал ложную отбраковку.
    ponytail: ESCAPE, отличный от QUOTE (`\\"`), чётность ломает; понадобится — полный разбор.
    """

    def __init__(self, options):
        self.is_csv = re.search(r"\bCSV\b", options, re.I) is not None
        self.header = self.is_csv and re.search(r"\bHEADER\b", options, re.I) is not None
        self.delimiter = _opt_char(options, "DELIMITER", "," if self.is_csv else "\t")
        self.quote = _opt_char(options, "QUOTE", '"') if self.is_csv else None
        self._q = self.quote.encode() if self.quote else None
        self.records = 0
        self._inq = False       # внутри кавычек
        self._open = False      # после конца последней записи были байты
        self._head = bytearray()

    def feed(self, chunk):
        parts = chunk.split(b"\n")
        for i, part in enumerate(parts):
            if self._q:
                self._inq ^= part.count(self._q) & 1
            if part.strip(b"\r"):
                self._open = True
            if not self.records and len(self._head) < CHUNK:
                self._head += part + (b"\n" if i < len(parts) - 1 else b"")
            if i < len(parts) - 1 and not self._inq and self._open:
                self.records += 1
                self._open = False
        return chunk

    def rows(self):
        """Записи данных: с незакрытой последней строкой, без заголовка."""
        n = self.records + (1 if self._open else 0)
        return max(n - 1, 0) if self.header else n

    def columns(self):
        """Имена из заголовка (`HEADER` в options), иначе пусто."""
        if not self.header or not self._head:
            return []
        text = bytes(self._head).decode("utf-8", "replace")
        return next(csv.reader(io.StringIO(text), delimiter=self.delimiter, quotechar=self.quote), [])


class HrpS3ToPgStreamOperator(BaseOperator):
    """Загружает объект S3 в таблицу GP/PG одной транзакцией, потоково."""

    template_fields = ("s3_key", "table", "bucket")

    def __init__(self, *, s3_key, table, bucket=None, s3_conn_id="s3", conn_id="cap_gp_etl",
                 format="auto", root=None, item=None, options="CSV HEADER DELIMITER ','",
                 truncate=None, file_name_column=None, statement_timeout=240, **kwargs):
        # Ошибка параметра — здесь, до S3 и базы: даг падает на разборе, а не посреди загрузки.
        if truncate not in TRUNCATE_MODES:
            raise ValueError(f"truncate={truncate!r}: ждём None, 'soft', 'hard' или 'auto'")
        if format not in FORMATS:
            raise ValueError(f"format={format!r}: ждём csv, json, jsonl или auto")
        super().__init__(**kwargs)
        self.s3_key, self.table, self.bucket = s3_key, table, bucket
        self.s3_conn_id, self.conn_id = s3_conn_id, conn_id
        self.format, self.root, self.item = format, root, item
        self.options, self.truncate = options, truncate
        self.file_name_column = file_name_column
        self.statement_timeout = int(statement_timeout or 0)

    def _log_notices(self, conn):
        """Уведомления сервера — в лог; возвращает (их число, отбраковка GP из них или None)."""
        notices, rejected = list(conn.notices), None
        del conn.notices[:]
        for n in notices:
            self.log.info("сервер: %s", n.strip())
            m = _GP_REJECTED.search(n)
            if m:
                rejected = (rejected or 0) + int(m.group(1))
        return len(notices), rejected

    def execute(self, context):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        key = self.s3_key
        if re.search(r"\.(gz|zip)$", key, re.I):
            raise NotImplementedError(f"{key}: сжатие gz/zip пока не поддерживается")
        fmt = detect_format(key, self.format)
        if fmt != "csv" or self.file_name_column:
            raise NotImplementedError(f"{key}: пока только csv без file_name_column")
        tbl = parse_table(self.table)

        obj = S3Hook(aws_conn_id=self.s3_conn_id).get_key(key, self.bucket)
        if obj is None:
            raise AirflowFailException(f"нет объекта s3://{self.bucket or ''}/{key}")

        # closing — with по соединению psycopg2 закрывает транзакцию, а не соединение.
        with closing(PostgresHook(postgres_conn_id=self.conn_id).get_conn()) as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    timeout = f"{self.statement_timeout}min" if self.statement_timeout > 0 else "0"
                    cur.execute("set local statement_timeout = %s", (timeout,))
                    self.log.info("statement_timeout %s", timeout)

                    cur.execute(sql.SQL("select * from {} limit 0").format(tbl))
                    target = _target_types(cur, cur.description)

                    how = _truncate(cur, tbl, self.truncate)
                    self.log.info("усечение: %s", how or "нет")

                    counter = CsvCounter(self.options)
                    copy = sql.SQL("copy {} from stdin with " + self.options).format(tbl).as_string(conn)
                    with closing(obj.get()["Body"]) as body:
                        # Пустой кусок s3_IterStream принимает за конец потока — отсеиваем.
                        stream = s3_IterStream(counter.feed(c) for c in body.iter_chunks(CHUNK) if c)
                        cur.copy_expert(copy, stream, size=CHUNK)
                    # GP 6.25 при SEGMENT REJECT LIMIT отвечает «COPY n» вместе с отбракованными
                    # строками (стенд 6.25.4: COPY 3, в таблице 2); их число есть только в NOTICE.
                    # NOTICE от COPY psycopg2 кладёт в conn.notices лишь после следующего запроса.
                    loaded = cur.rowcount
                    # Текст сверен только на стенде (английский); при другом языке сервера число
                    # не распознаётся — тогда предупреждение, а не молчаливый 0.
                    cur.execute("select 1")
                    seen, gp_rejected = self._log_notices(conn)
                    if gp_rejected is not None:
                        loaded -= gp_rejected
                    elif seen and _REJECT_LIMIT.search(self.options):
                        self.log.warning("⚠️ уведомление сервера об отбраковке не распознано: "
                                         "rows_loaded может включать отбракованные строки")
                conn.commit()
            except BaseException:
                conn.rollback()
                self._log_notices(conn)
                raise

        source_rows = counter.rows()
        rejected = source_rows - loaded
        self.log.info("прочитано %s, загружено %s в %s", source_rows, loaded, self.table)
        if rejected:
            self.log.warning("⚠️ не загружено строк: %s (отбраковка COPY)", rejected)
        return {
            "rows_source": source_rows, "rows_loaded": loaded, "rows_rejected": rejected,
            "files": [key], "target": target,
            "source": {c: "text" for c in counter.columns()},
            "unmapped": [], "defaulted": [],
        }


def _selfcheck():
    for bad in ("a;b", "a.b.c", "", "1t"):
        try:
            parse_table(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(bad)
    assert detect_format("x/y.csv", "auto") == "csv"
    assert detect_format("y.ndjson", "auto") == "jsonl"
    assert detect_format("y.txt", "csv") == "csv"
    try:
        detect_format("y.txt", "auto")
    except ValueError as e:
        assert "y.txt" in str(e)
    else:
        raise AssertionError("y.txt")
    try:
        HrpS3ToPgStreamOperator(task_id="t", s3_key="k", table="t", truncate="yes")
    except ValueError:
        pass
    else:
        raise AssertionError("truncate=yes")

    data = b'id;name\r\n1;"a\nb"\r\n2;"x ""q"" \n\n y"\r\n3;z'
    for n in (1, 2, 5, 1000):
        c = CsvCounter("CSV HEADER DELIMITER ';'")
        for i in range(0, len(data), n):
            c.feed(data[i:i + n])
        assert (c.rows(), c.columns()) == (3, ["id", "name"]), (n, c.rows(), c.columns())
    c = CsvCounter("CSV")
    c.feed(b"1,2\n3,4\n")
    assert (c.rows(), c.columns()) == (2, [])
    assert _opt_char("csv delimiter E'\\t'", "DELIMITER", ",") == "\t"
    print("selfcheck ok")


if __name__ == "__main__":
    _selfcheck()
