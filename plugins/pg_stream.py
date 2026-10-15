"""###🛠️ Потоковая загрузка S3 → GP/PG (`plugins/pg_stream.py`)
*2026-10-15 13:26 MSK · v0.4 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

`HrpS3ToPgStreamOperator` — один объект S3 в одну таблицу Greenplum или PostgreSQL:
потоково, одной транзакцией, одной командой COPY на файл. Усечение приёмника —
`truncate` = None | soft | hard | auto. Итог (строки источника и загруженные, типы
колонок приёмника) — в XCom `return_value`.

Сейчас — CSV и JSON / JSON Lines (`root`, `item`) в приёмник из одной колонки json/jsonb, без
сжатия; раскладка по колонкам, gz/zip и `file_name_column` — дальше.
Самопроверка: `python3 plugins/pg_stream.py`.
"""
from __future__ import annotations

import codecs
import csv
import io
import json
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
_JSON_TYPES = (114, 3802)   # oid json, jsonb
# Текстовый формат COPY съедает обратные слэши (\" и \n внутри JSON ломаются), поэтому csv
# с кавычкой и разделителем, которых в JSON не бывает: строка уходит в колонку как есть.
_JSON_COPY = "copy {} ({}) from stdin with (format csv, quote e'\\x01', delimiter e'\\x02')"
MAX_KEYS = 500              # ключей в итоге `source`: сверх — source_truncated


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


_WS = " \t\r\n"
_NUM = set("0123456789.eE+-")
MAX_ITEM = 64 << 20  # символов: дольше элемент не дочитывается — значит, он битый


def iter_json_array(chunks, root: str = ""):
    """Потоково отдаёт элементы JSON-массива из кусков текста.

    root — массив лежит в объекте под этим ключом, и ключ в объекте единственный:
    `{"data": [...]}` (конверт Kafka Connect у файлов ТФС → GP). Пусто — массив и есть файл.

    Память — один элемент плюс кусок, а не весь файл: json.load на гигабайтном массиве
    кладёт воркер по OOM. ijson не взят — его нет в образах, а новая зависимость на
    контурах идёт через МПР. Синтаксис строгий: запятая ровно между элементами, после `]`
    только пробелы — иначе ValueError, а не молча урезанная загрузка.
    """
    # [obj → key → colon →] start → first → item ⇄ sep → [close →] end
    dec, buf, state = json.JSONDecoder(), "", "obj" if root else "start"
    for chunk in chunks:
        buf += chunk
        pos = 0
        while True:
            while pos < len(buf) and buf[pos] in _WS:
                pos += 1
            if pos == len(buf):
                break
            ch = buf[pos]
            if state == "end":
                raise ValueError(f"данные после конца массива: {buf[pos:pos + 40]!r}")
            if state in ("obj", "colon", "close"):
                want = {"obj": "{", "colon": ":", "close": "}"}[state]
                if ch != want:
                    raise ValueError(f"ожидалось {want} конверта {root!r}: {buf[pos:pos + 40]!r}")
                state, pos = {"obj": "key", "colon": "start", "close": "end"}[state], pos + 1
                continue
            if state == "start":
                if ch != "[":
                    raise ValueError("ожидался JSON-массив")
                state, pos = "first", pos + 1
                continue
            if ch == "]" and state in ("first", "sep"):
                state, pos = "close" if root else "end", pos + 1
                continue
            if state == "sep":
                if ch != ",":
                    raise ValueError(f"ожидалась , или ]: {buf[pos:pos + 40]!r}")
                state, pos = "item", pos + 1
                continue
            if ch in ",]}:":                # значение с них не начинается: «,,», «[1,]» — сразу, не копя буфер
                raise ValueError(f"ожидался элемент массива: {buf[pos:pos + 40]!r}")
            try:
                obj, end = dec.raw_decode(buf, pos)
            except json.JSONDecodeError as exc:
                if len(buf) - pos > MAX_ITEM:
                    raise ValueError(f"битый элемент: {exc}") from exc
                break                      # элемент не дочитан — ждём кусок
            if type(obj) in (int, float) and not set(buf[end:]) - _NUM:
                break                      # число могло оборваться на границе куска: «12.» → 12
            if state == "key":
                if obj != root:
                    raise ValueError(f"в конверте ключ {obj!r}, а не {root!r}")
                state, pos = "colon", end
                continue
            yield obj
            state, pos = "sep", end
        buf = buf[pos:]
    if state != "end":
        raise ValueError(f"файл оборван: {buf[:40]!r}" if buf else f"файл оборван (разбор остановился на {state})")


def iter_jsonl(chunks):
    """JSON Lines из кусков текста: элемент на каждой непустой строке; битая — ValueError с номером."""
    buf, n = "", 0
    for chunk in chunks:
        buf += chunk
        *lines, buf = buf.split("\n")
        for line in lines:
            n += 1
            yield from _jsonl_line(line, n)
    yield from _jsonl_line(buf, n + 1)


def _jsonl_line(line, n):
    if line.strip():
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"строка {n}: {exc}") from exc


def _text(body):
    """Текст объекта S3 кусками; инкрементальный декодер не рвёт кириллицу на границе куска."""
    utf8 = codecs.getincrementaldecoder("utf-8")()
    for chunk in body.iter_chunks(CHUNK):
        yield utf8.decode(chunk)
    yield utf8.decode(b"", final=True)


def parse_item(item):
    """Путь `item`: `"a.b"` → `["a", "b"]`; список — как есть (ключ с точкой); пусто — весь элемент."""
    if not item:
        return []
    return list(item) if isinstance(item, (list, tuple)) else item.split(".")


def pick(obj, path, n):
    """Объект по пути `path` внутри элемента номер `n`; нет ключа или не объект — ValueError."""
    cur = obj
    for i, k in enumerate(path):
        where = ".".join(path[:i + 1])
        if not isinstance(cur, dict):
            raise ValueError(f"элемент {n}: {where}: уровень выше — не объект")
        if k not in cur:
            raise ValueError(f"элемент {n}: {where}: нет ключа {k}")
        cur = cur[k]
    return cur


_TYPE_ORDER = ("string", "number", "boolean", "object", "array", "null")


def _json_type(v):
    if isinstance(v, str):
        return "string"
    if isinstance(v, bool):         # до int: bool — подкласс int
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return "null"


class SourceStats:
    """Ключи объектов источника и их типы JSON — для `source` в итоге (не больше MAX_KEYS)."""

    def __init__(self):
        self.keys = {}
        self.truncated = False

    def add(self, obj):
        for k, v in obj.items():
            types = self.keys.get(k)
            if types is None:
                if len(self.keys) >= MAX_KEYS:
                    self.truncated = True
                    continue
                types = self.keys[k] = set()
            types.add(_json_type(v))

    def as_dict(self):
        return {k: "|".join(t for t in _TYPE_ORDER if t in ts) for k, ts in self.keys.items()}


def _copy_line(obj):
    # json.dumps экранирует управляющие символы, поэтому \x01 и \x02 из _JSON_COPY в строке не встретятся
    return json.dumps(obj, ensure_ascii=False).encode() + b"\n"


def _batched(lines, size=CHUNK):
    """Строки в куски ~size байт: s3_IterStream склеивает буфер на каждом куске, мелкие — квадратично."""
    buf = bytearray()
    for line in lines:
        buf += line
        if len(buf) >= size:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)


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

    def source(self):
        """Колонки файла для итога: в CSV все значения — текст."""
        return {c: "text" for c in self.columns()}

    def columns(self):
        """Имена из заголовка (`HEADER` в options), иначе пусто."""
        if not self.header or not self._head:
            return []
        text = bytes(self._head).decode("utf-8", "replace")
        return next(csv.reader(io.StringIO(text), delimiter=self.delimiter, quotechar=self.quote), [])


class _JsonFeed:
    """Элементы JSON / JSON Lines → строки COPY объекта `item`; считает элементы и типы ключей."""

    def __init__(self, fmt, root, path):
        self.fmt, self.root, self.path = fmt, root or "", path
        self.n = 0
        self.stats = SourceStats()

    def lines(self, text):
        items = iter_jsonl(text) if self.fmt == "jsonl" else iter_json_array(text, self.root)
        for el in items:
            self.n += 1
            obj = pick(el, self.path, self.n)
            if isinstance(obj, dict):
                self.stats.add(obj)
            yield _copy_line(obj)

    def rows(self):
        return self.n

    def source(self):
        return self.stats.as_dict()


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
        if self.file_name_column:
            raise NotImplementedError("file_name_column пока не поддерживается")
        tbl = parse_table(self.table)
        path = parse_item(self.item)

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
                    desc = cur.description
                    target = _target_types(cur, desc)
                    if fmt != "csv" and (len(desc) != 1 or desc[0].type_code not in _JSON_TYPES):
                        raise NotImplementedError(f"{self.table}: раскладка JSON по колонкам пока не "
                                                  "поддерживается — нужна одна колонка json/jsonb")

                    how = _truncate(cur, tbl, self.truncate)
                    self.log.info("усечение: %s", how or "нет")

                    with closing(obj.get()["Body"]) as body:
                        if fmt == "csv":
                            counter = CsvCounter(self.options)
                            copy = sql.SQL("copy {} from stdin with " + self.options).format(tbl)
                            # Пустой кусок s3_IterStream принимает за конец потока — отсеиваем.
                            stream = (counter.feed(c) for c in body.iter_chunks(CHUNK) if c)
                        else:
                            counter = _JsonFeed(fmt, self.root, path)
                            copy = sql.SQL(_JSON_COPY).format(tbl, sql.Identifier(desc[0].name))
                            stream = _batched(counter.lines(_text(body)))
                        cur.copy_expert(copy.as_string(conn), s3_IterStream(stream), size=CHUNK)
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
        res = {
            "rows_source": source_rows, "rows_loaded": loaded, "rows_rejected": rejected,
            "files": [key], "target": target, "source": counter.source(),
            "unmapped": [], "defaulted": [],
        }
        if isinstance(counter, _JsonFeed) and counter.stats.truncated:
            res["source_truncated"] = True
        return res


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

    assert list(iter_json_array(['[1,', '2]'])) == [1, 2]
    assert list(iter_json_array(['{"data": [{"a":1}]}'], "data")) == [{"a": 1}]
    for bad in ('[1,,2]', '[1,]'):
        try:
            list(iter_json_array([bad, "1" * 100]))   # ошибка на месте, а не «файл оборван» в конце
        except ValueError as e:
            assert "ожидался элемент" in str(e), e
        else:
            raise AssertionError(bad)
    for bad in ('[1] x', '[1,', '{"data": [1]}', '[1 2]', '[,1]'):
        try:
            list(iter_json_array([bad]))
        except ValueError:
            pass
        else:
            raise AssertionError(bad)
    data = [x for i in range(30) for x in ({"id": i, "s": 'Имя "q" \\ ,] \n\t'}, 7, 12.5e3, [1, 2], None, True)]
    text = json.dumps(data, ensure_ascii=False, indent=2)
    for n in (1, 3, 17, 4096):
        assert list(iter_json_array(text[i:i + n] for i in range(0, len(text), n))) == data, n
    assert list(iter_jsonl(['{"a":1}\n{"a"', ':2}\n\n'])) == [{"a": 1}, {"a": 2}]
    assert list(iter_jsonl(['{"a":1}\r\n2'])) == [{"a": 1}, 2]
    try:
        list(iter_jsonl(['{"a":1}\n{bad\n']))
    except ValueError as e:
        assert "строка 2" in str(e), e
    else:
        raise AssertionError("битая строка jsonl")
    st = SourceStats()
    st.add({"a": 1, "b": True})
    st.add({"a": None, "c": [1]})
    assert st.as_dict() == {"a": "number|null", "b": "boolean", "c": "array"}, st.as_dict()
    big = SourceStats()
    big.add({f"k{i}": 1 for i in range(600)})
    assert len(big.as_dict()) == 500 and big.truncated
    assert parse_item("payload.value") == ["payload", "value"]
    assert parse_item(["p", "a.b"]) == ["p", "a.b"] and parse_item(None) == parse_item("") == []
    assert pick({"p": {"v": {"x": 1}}}, ["p", "v"], 7) == {"x": 1}
    for el, path in (({"p": {}}, ["p", "nope"]), ({"p": 1}, ["p", "nope"])):
        try:
            pick(el, path, 7)
        except ValueError as e:
            assert "7" in str(e) and "p.nope" in str(e), e
        else:
            raise AssertionError(path)
    line = _copy_line({"s": "a\x01b\x02\n\"", "к": 1})
    assert b"\x01" not in line and b"\x02" not in line and line.count(b"\n") == 1, line
    assert b"".join(_batched([b"ab", b"cd", b"e"], 3)) == b"abcde"
    assert list(_batched([b"ab", b"cd", b"e"], 3)) == [b"abcd", b"e"]
    feed = _JsonFeed("json", "data", ["payload"])
    out = list(feed.lines(['{"data": [{"schema": 1, "payload": {"x": "я"}}, {"payload": {"x": null}}]}']))
    assert [json.loads(x) for x in out] == [{"x": "я"}, {"x": None}] and feed.rows() == 2
    assert feed.source() == {"x": "string|null"}, feed.source()
    print("selfcheck ok")


if __name__ == "__main__":
    _selfcheck()
