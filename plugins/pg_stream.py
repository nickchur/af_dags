"""###🛠️ Потоковая загрузка S3 → GP/PG (`plugins/pg_stream.py`)
*2026-10-15 15:22 MSK · v0.12 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

`HrpS3ToPgStreamOperator` — один объект S3 в одну таблицу Greenplum или PostgreSQL:
потоково, одной транзакцией, одной командой COPY на файл. Для баз, где длинная транзакция
допустима: память воркера не зависит от размера файла. Итог — в XCom `return_value`.

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `s3_key` | — | ключ объекта; `.gz` — поток gzip, `.zip` — все файлы архива одной транзакцией |
| `table` | — | приёмник `схема.таблица` |
| `bucket` / `s3_conn_id` | `None` / `s3` | бакет (без него — из подключения) |
| `conn_id` | `cap_gp_etl` | подключение GP/PG; прав на приёмник у него должно хватать |
| `format` | `auto` | `csv` / `json` (массив) / `jsonl`; `auto` — по расширению (`.csv`, `.json`, `.jsonl`, `.ndjson`) |
| `root` | `None` | ключ массива в конверте `{"data": [...]}` (json) |
| `item` | `None` | путь к объекту внутри элемента через точку (`payload`, `a.b`) |
| `options` | `CSV HEADER DELIMITER ','` | опции COPY для csv (в т. ч. `LOG ERRORS SEGMENT REJECT LIMIT` на GP) |
| `truncate` | `None` | `hard` — truncate, `soft` — delete, `auto` — truncate, если приёмник не занят, иначе delete |
| `file_name_column` | `None` | колонка приёмника для ключа S3 (имени файла в архиве) |
| `statement_timeout` | `240` | минуты; `0` — без ограничения |
| `tmp_table` | `None` | json/jsonl: постоянная таблица сырья `(item jsonb, file_name text)`, см. ниже |

Приёмник из одной колонки json/jsonb получает объект целиком, иначе — раскладка по колонкам
(`select * limit 0`, `jsonb_populate_record`, ключи без учёта регистра; лишние ключи — в лог и
`unmapped`; колонки, не встреченные ни в одном элементе, — `default` из DDL, а встреченная колонка
заполняется во всех строках: где ключа нет — NULL). CSV без `file_name_column` идёт прямо в
приёмник: колонки файла = колонки таблицы.

`tmp_table`: COPY из S3 коммитится в сырьё отдельно, приёмник меняется второй, короткой
транзакцией; её сбой оставляет сырьё до повтора задачи (повтор очищает `tmp_table` и
перечитывает файл из S3). Сырьё — элементы после раскладки: ключи уже названы колонками
приёмника, лишние отброшены (они — в логе и `unmapped`). Загрузки с одной `tmp_table` идут по очереди
(advisory-блокировка на обе транзакции, ожидание — с предупреждением в логе).
Без `tmp_table` сырьё json — во временной `tmp_stream_raw` той же транзакции.

Итог: `rows_source`, `rows_loaded`, `rows_rejected`, `files`, `target` и `source` (типы колонок
приёмника и полей источника), `unmapped`, `defaulted`[, `source_truncated`].

```python
HrpS3ToPgStreamOperator(
    task_id="load", conn_id="cap_gp_etl", bucket="tfs-in",
    s3_key="{{ params.key }}", table="s_grnplm_vd_hr_edp_dia.tb_scpl",
    root="data", item="payload", truncate="auto", file_name_column="src_file",
)
```

Драйвер — какой отдаёт `PostgresHook`: psycopg2 (AF2) или psycopg 3 (AF3, провайдер postgres 7 при
SQLAlchemy 2); SQL собирается строкой с идентификаторами в кавычках.

Ограничения: на сигме — только после переноса в etl-core и строки в `allowed_operators.txt`.
Внутри `.zip` сжатые файлы не поддерживаются. Элемент JSON (строка jsonl) длиннее 64 млн
символов считается битым. Ошибка разбора (битый JSON, кодировка, gzip)
поднимается исходной — `ValueError` и т. п., а не `QueryCanceled` от COPY; нет объекта в S3 —
`AirflowFailException` без повторов.
Самопроверка: `python3 plugins/pg_stream.py`; сценарии — даг `tools_test_pg_stream`.
"""
from __future__ import annotations

import codecs
import csv
import gzip
import io
import json
import os
import re
import types
from contextlib import closing

import psycopg2
from botocore.exceptions import ClientError

try:
    import psycopg  # AF3: PostgresHook (провайдер postgres 7 + SQLAlchemy 2) отдаёт соединение psycopg 3
except ImportError:
    psycopg = None

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowFailException

try:
    from plugins.s3_utils import s3_IterStream  # type: ignore
except ImportError:
    from CI06932748.tools.s3_utils import s3_IterStream  # type: ignore

TRUNCATE_MODES = (None, "soft", "hard", "auto")
FORMATS = ("auto", "csv", "json", "jsonl")
CHUNK = 1 << 20     # байт за одно чтение из S3 и за одно чтение COPY

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$")
_EXT = {".csv": "csv", ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl"}
_GP_REJECTED = re.compile(r"found (\d+) data formatting errors", re.I)
_REJECT_LIMIT = re.compile(r"\bREJECT\s+LIMIT\b", re.I)
_OPT_CHAR = r"\b{}\s+(?:AS\s+)?E?'((?:\\.|[^'])+)'"
_JSON_TYPES = (114, 3802)   # oid json, jsonb
# Текстовый формат COPY съедает обратные слэши (\" и \n внутри JSON ломаются), поэтому csv
# с кавычкой и разделителем, которых в JSON не бывает: строка уходит в колонку как есть.
_JSON_COPY = "copy {} ({}) from stdin with (format csv, quote e'\\x01', delimiter e'\\x02')"
# Ошибки сервера обоих драйверов: соединение — psycopg2 (AF2) или psycopg 3 (AF3)
_DB_ERRORS = (psycopg2.Error,) + ((psycopg.Error,) if psycopg else ())
MAX_KEYS = 500              # ключей в итоге `source`: сверх — source_truncated


def parse_table(table):
    """`схема.таблица` или `таблица` → безопасный идентификатор; иное — ValueError."""
    if not _IDENT.match(table or ""):
        raise ValueError(f"недопустимое имя таблицы: {table!r}")
    return ".".join(map(_ident, table.split(".")))


def _ident(name):
    """Идентификатор в кавычках. SQL собирается строкой: модуль `sql` у psycopg2 и psycopg 3 свой."""
    return '"' + name.replace('"', '""') + '"'


def _sqlstate(exc):
    return getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)


def _notice_sink(conn):
    """Список, куда соединение складывает уведомления сервера: у psycopg2 это conn.notices."""
    if hasattr(conn, "notices"):
        return conn.notices
    sink = []
    conn.add_notice_handler(lambda d: sink.append(f"{d.severity}:  {d.message_primary}"))
    return sink


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
                # Недочитанный элемент ломается у конца буфера (хвост литерала, \uXXXX — до 6
                # знаков) или незакрытой строкой; ошибка раньше — элемент битый, буфер не копим.
                if len(buf) - pos > MAX_ITEM or (exc.pos < len(buf) - 8
                                                 and not exc.msg.startswith("Unterminated string")):
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
        if len(buf) > MAX_ITEM:     # не JSON Lines (массив в одну строку) — падение, а не OOM
            raise ValueError(f"строка {n + 1} длиннее {MAX_ITEM} символов")
    yield from _jsonl_line(buf, n + 1)


def _jsonl_line(line, n):
    if line.strip():
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"строка {n}: {exc}") from exc


def _no_bom(chunks):
    """Байты без UTF-8 BOM в начале (выгрузки Windows): в CSV он лёг бы в первое значение."""
    it, head = iter(chunks), b""
    for c in it:
        head += c
        if len(head) >= 3:
            break
    head = head[3:] if head.startswith(codecs.BOM_UTF8) else head
    if head:            # пустой кусок s3_IterStream принимает за конец
        yield head
    yield from it


def _text(chunks):
    """Текст из кусков байт; инкрементальный декодер не рвёт кириллицу на границе куска."""
    utf8 = codecs.getincrementaldecoder("utf-8-sig")()     # BOM выгрузок Windows — отбросить
    for chunk in chunks:
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


def _copy_line(obj, tail=b"\n"):
    # json.dumps экранирует управляющие символы, поэтому \x01 и \x02 из _JSON_COPY в строке не встретятся
    return json.dumps(obj, ensure_ascii=False).encode() + tail


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
        cur.execute("truncate " + tbl)
        return "truncate"
    if mode == "soft":
        cur.execute("delete from " + tbl)
        return "delete"
    # auto — как srv_wf.pr_soft_truncate, но без функций схемы: их нет на PG и у читающих
    # подключений. TRUNCATE держит ACCESS EXCLUSIVE до конца загрузки, поэтому берём его,
    # только если таблица свободна сейчас; занята — DELETE, читатели не ждут.
    cur.execute("savepoint pg_stream_trunc")
    try:
        cur.execute("lock table " + tbl + " in access exclusive mode nowait")
        cur.execute("truncate " + tbl)
        cur.execute("release savepoint pg_stream_trunc")
        return "truncate"
    except _DB_ERRORS as exc:
        if _sqlstate(exc) != "55P03":       # lock_not_available
            raise
        cur.execute("rollback to savepoint pg_stream_trunc")
        cur.execute("delete from " + tbl)
        return "delete"


def _target_types(cur, description):
    """Колонки приёмника с короткими именами типов (`int4`, `varchar`) — как srv_wf.pr_get_format_type."""
    cur.execute("select oid, typname::text from pg_catalog.pg_type where oid::bigint = any(%s)",
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


class KeyMap:
    """Ключи объекта → колонки приёмника без учёта регистра (REQ-24).

    Переименование здесь, а не в SQL: jsonb_populate_record ищет ключ с точным именем колонки.
    `seen` — колонки, встреченные хоть раз: только они идут в insert, остальные берут default
    из DDL (REQ-27), а не NULL от jsonb_populate_record.
    """

    def __init__(self, columns):
        self.cols = {c.lower(): c for c in columns}
        self.extra, self.seen = set(), set()

    def map(self, obj, n):
        out, low = {}, {}
        for k, v in obj.items():
            lk = k.lower()
            if lk in low:
                raise ValueError(f"элемент {n}: ключи {low[lk]} и {k} различаются только регистром")
            low[lk] = k
            col = self.cols.get(lk)
            if col is None:
                self.extra.add(k)
            else:
                out[col] = v
        self.seen.update(out)
        return out


def _plain(name):
    """Имя без расширения сжатия: формат решает то, что под ним."""
    return re.sub(r"\.(gz|zip)$", "", name, flags=re.I)


def _zip_name(raw):
    # Флаг UTF-8 stream_unzip не отдаёт: пробуем UTF-8, иначе cp866 (архиватор Windows, как s3_from_zip)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp866", errors="replace")


def iter_sources(body, key):
    """Пары (имя файла, куски байт): `.gz` распаковывается, `.zip` — каждый файл архива по очереди.

    Куски файла из zip надо дочитать до следующего файла — COPY читает поток до конца.
    """
    def raw():
        return iter(lambda: body.read(CHUNK), b"")

    low = key.lower()
    if low.endswith(".gz"):
        gz = gzip.GzipFile(fileobj=body)
        yield key, iter(lambda: gz.read(CHUNK), b"")
    elif low.endswith(".zip"):
        from stream_unzip import stream_unzip  # type: ignore
        for name, _size, chunks in stream_unzip(raw()):
            name = _zip_name(name)
            if name.endswith("/"):
                for _ in chunks:        # каталог: пусто, но поток архива надо промотать
                    pass
                continue
            if re.search(r"\.(gz|zip)$", name, re.I):
                raise ValueError(f"{key}/{name}: сжатие внутри архива не поддерживается")
            yield f"{key}/{name}", chunks
    else:
        yield key, raw()


class _JsonFeed:
    """Элементы JSON / JSON Lines → строки COPY объекта `item`; считает элементы и типы ключей.

    С KeyMap — раскладка: ключи переименованы в колонки, лишние отброшены.
    """

    def __init__(self, root, path, km=None):
        self.root, self.path, self.km = root or "", path, km
        self.n = 0
        self.stats = SourceStats()

    def lines(self, fmt, text, tail=b"\n"):
        items = iter_jsonl(text) if fmt == "jsonl" else iter_json_array(text, self.root)
        for el in items:
            self.n += 1
            obj = pick(el, self.path, self.n)
            if isinstance(obj, dict):
                self.stats.add(obj)
            if self.km is not None:
                if not isinstance(obj, dict):
                    raise ValueError(f"элемент {self.n}: не объект — по колонкам не разложить")
                obj = self.km.map(obj, self.n)
            yield _copy_line(obj, tail)


def _tail(name):
    """Хвост строки COPY с именем файла второй колонкой (разделитель \\x02 из _JSON_COPY)."""
    if re.search(r"[\x00-\x1f]", name):
        raise ValueError(f"управляющий символ в имени файла: {name!r}")
    return b"\x02" + name.encode() + b"\n"


class HrpS3ToPgStreamOperator(BaseOperator):
    """Загружает объект S3 в таблицу GP/PG одной транзакцией, потоково."""

    template_fields = ("s3_key", "table", "bucket")

    def __init__(self, *, s3_key, table, bucket=None, s3_conn_id="s3", conn_id="cap_gp_etl",
                 format="auto", root=None, item=None, options="CSV HEADER DELIMITER ','",
                 truncate=None, file_name_column=None, statement_timeout=240, tmp_table=None, **kwargs):
        # Ошибка параметра — здесь, до S3 и базы: даг падает на разборе, а не посреди загрузки.
        if truncate not in TRUNCATE_MODES:
            raise ValueError(f"truncate={truncate!r}: ждём None, 'soft', 'hard' или 'auto'")
        if format not in FORMATS:
            raise ValueError(f"format={format!r}: ждём csv, json, jsonl или auto")
        if int(statement_timeout or 0) < 0:
            raise ValueError(f"statement_timeout={statement_timeout!r}: минуты, 0 — без ограничения")
        if tmp_table:
            parse_table(tmp_table)
            if format == "csv":
                raise ValueError("tmp_table — только для json и jsonl")
        super().__init__(**kwargs)
        self.s3_key, self.table, self.bucket = s3_key, table, bucket
        self.s3_conn_id, self.conn_id = s3_conn_id, conn_id
        self.format, self.root, self.item = format, root, item
        self.options, self.truncate = options, truncate
        self.file_name_column = file_name_column
        self.statement_timeout = int(statement_timeout or 0)
        self.tmp_table = tmp_table

    def _log_notices(self):
        """Уведомления сервера — в лог; возвращает (их число, отбраковка GP из них или None)."""
        notices, rejected = list(self._notices), None
        del self._notices[:]
        for n in notices:
            self.log.info("сервер: %s", n.strip())
            m = _GP_REJECTED.search(n)
            if m:
                rejected = (rejected or 0) + int(m.group(1))
        return len(notices), rejected

    def _copy(self, cur, conn, copy, chunks, options=""):
        """Одна команда COPY из потока; возвращает принятые строки (без отбраковки GP)."""
        if _REJECT_LIMIT.search(options):   # число отбраковки приходит только NOTICE — не дать его отрезать
            cur.execute("set local client_min_messages = notice")
        failed = []

        def guard():
            # Исключение внутри read() psycopg2 отдаёт как QueryCanceled «error in .read() call»
            # (тот же SQLSTATE, что у statement_timeout): исходное поднимаем сами.
            try:
                yield from chunks
            except GeneratorExit:   # закрытие генератора при ошибке базы — не ошибка источника
                raise
            except BaseException as exc:
                failed.append(exc)
                raise

        try:
            if hasattr(cur, "copy_expert"):     # psycopg2
                cur.copy_expert(copy, s3_IterStream(guard()), size=CHUNK)
            else:                               # psycopg 3
                with cur.copy(copy) as cp:
                    for chunk in guard():
                        cp.write(chunk)
        except _DB_ERRORS:
            if failed:
                raise failed[0] from None
            raise
        # GP 6.25 при SEGMENT REJECT LIMIT отвечает «COPY n» вместе с отбракованными
        # строками (стенд 6.25.4: COPY 3, в таблице 2); их число есть только в NOTICE.
        # NOTICE от COPY psycopg2 кладёт в conn.notices лишь после следующего запроса.
        loaded = cur.rowcount
        # Текст сверен только на стенде (английский); при другом языке сервера число
        # не распознаётся — тогда предупреждение, а не молчаливый 0.
        cur.execute("select 1")
        seen, gp_rejected = self._log_notices()
        if gp_rejected is not None:
            return loaded - gp_rejected
        if seen and _REJECT_LIMIT.search(options):
            self.log.warning("⚠️ уведомление сервера об отбраковке не распознано: "
                             "rows_loaded может включать отбракованные строки")
        return loaded

    def execute(self, context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        key = self.s3_key
        if not key.lower().endswith(".zip"):
            fmt = detect_format(_plain(key), self.format)     # .txt без format — ошибка до S3
            if fmt == "csv" and self.tmp_table:
                raise ValueError(f"{key}: tmp_table — только для json и jsonl")
        tbl = parse_table(self.table)

        # closing — with по соединению psycopg2 закрывает транзакцию, а не соединение.
        with closing(PostgresHook(postgres_conn_id=self.conn_id).get_conn()) as conn:
            conn.autocommit = False
            self._notices = _notice_sink(conn)
            with conn.cursor() as cur:
                if not self.tmp_table:
                    res = self._tx(conn, lambda: self._one_tx(cur, conn, key, tbl))
                else:
                    raw = parse_table(self.tmp_table)
                    lock = self._tx(conn, lambda: self._lock_raw(cur, raw))
                    try:
                        # 1: сырьё из S3 в tmp_table — долгая часть, приёмник не тронут
                        ctx = self._tx(conn, lambda: self._raw_tx(cur, conn, key, tbl, raw))
                        # 2: приёмник — только SQL внутри базы; сбой оставляет сырьё для разбора
                        try:
                            res = self._tx(conn, lambda: self._target_tx(cur, ctx, tbl, raw))
                        except BaseException:
                            # при сбое самого commit исход неизвестен — «не изменён» не утверждаем
                            self.log.error("вторая транзакция (приёмник %s) не прошла, сырьё файла — в %s до повтора задачи",
                                           self.table, self.tmp_table)
                            raise
                    finally:
                        try:
                            cur.execute("select pg_advisory_unlock(%s)", (lock,))
                            unlocked = cur.fetchone()[0]
                            conn.commit()
                        except _DB_ERRORS as exc:    # сеанс оборван — блокировка ушла вместе с ним
                            self.log.warning("⚠️ блокировка %s не снята явно: %s", self.tmp_table, exc)
                        else:
                            if not unlocked:    # другой серверный сеанс (пулер транзакций): очередь не работала
                                self.log.warning("⚠️ блокировка %s не найдена в сеансе при снятии", self.tmp_table)

        self.log.info("прочитано %s, загружено %s в %s", res["rows_source"], res["rows_loaded"], self.table)
        if res["rows_rejected"] > 0:
            self.log.warning("⚠️ не загружено строк: %s (отбраковка COPY)", res["rows_rejected"])
        elif res["rows_rejected"] < 0:     # счёт csv по кавычкам не видит ESCAPE, пустых строк, кодировок
            self.log.warning("⚠️ загружено больше, чем насчитано в источнике (%s): счёт строк csv "
                             "разошёлся с COPY", -res["rows_rejected"])
        return res

    def _tx(self, conn, body):
        """Транзакция: commit при успехе, rollback и уведомления сервера в лог при ошибке."""
        try:
            out = body()
            conn.commit()
            return out
        except BaseException:
            try:
                conn.rollback()
                self._log_notices()
            except _DB_ERRORS as exc:   # сеанс оборван: ошибкой таска остаётся исходная
                self.log.warning("⚠️ откат не выполнен: %s", exc)
            raise

    def _set_timeout(self, cur):
        timeout = f"{self.statement_timeout}min" if self.statement_timeout > 0 else "0"
        # литералом: psycopg 3 связывает параметры на сервере, а у SET их не бывает
        cur.execute(f"set local statement_timeout = '{timeout}'")
        self.log.info("statement_timeout %s", timeout)

    def _open(self):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        try:
            obj = S3Hook(aws_conn_id=self.s3_conn_id).get_key(self.s3_key, self.bucket)
        except ClientError as exc:      # get_key не возвращает None: load() бросает 404
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                raise AirflowFailException(f"нет объекта s3://{self.bucket or ''}/{self.s3_key}") from exc
            raise
        return closing(obj.get()["Body"])

    def _target(self, cur, tbl):
        """Состав приёмника (`select * limit 0`) и способ записи JSON (REQ-23)."""
        fcol = self.file_name_column
        cur.execute("select * from " + tbl + " limit 0")
        desc = cur.description
        target = _target_types(cur, desc)
        if fcol and fcol not in target:
            raise ValueError(f"file_name_column={fcol!r}: нет такой колонки в {self.table}")
        data = [c for c in desc if c.name != fcol]
        # одна json/jsonb-колонка — объект целиком в неё, иначе раскладка по колонкам
        one_json = len(data) == 1 and data[0].type_code in _JSON_TYPES
        km = None if one_json else KeyMap([c.name for c in data])
        return types.SimpleNamespace(target=target, data=data, one_json=one_json, km=km, fcol=fcol,
                                     feed=_JsonFeed(self.root, parse_item(self.item), km),
                                     files=[], rows=0, loaded=0, csv_source={}, raw=None)

    def _one_tx(self, cur, conn, key, tbl):
        """Без tmp_table: всё одной транзакцией, сырьё JSON — во временной tmp_stream_raw."""
        self._set_timeout(cur)
        ctx = self._target(cur, tbl)
        how = _truncate(cur, tbl, self.truncate)
        self.log.info("усечение: %s", how or "нет")
        self._fill(cur, conn, ctx, key, tbl)
        if ctx.raw is not None:
            ctx.loaded += self._insert_from_raw(cur, ctx, tbl, ctx.raw)
        return self._result(ctx)

    def _lock_raw(self, cur, raw):
        """Сеансовая блокировка tmp_table на обе транзакции: между ними таблица открыта, и чужая
        загрузка с той же tmp_table очистила бы её и подложила свой файл — приёмник получил бы
        чужие строки без ошибки. Ключ — oid таблицы: `raw` и `public.raw` — одна блокировка.
        Занята — предупреждение и ожидание в пределах statement_timeout."""
        self._set_timeout(cur)
        # имя — как его кавычит parse_table: строка как есть свернула бы регистр (s.Raw → s.raw)
        cur.execute("select to_regclass(%s)::oid::bigint", (raw,))
        lock = cur.fetchone()[0]
        if lock is None:
            raise ValueError(f"tmp_table {self.tmp_table}: таблицы нет")
        cur.execute("select pg_try_advisory_lock(%s)", (lock,))
        if not cur.fetchone()[0]:
            self.log.warning("⚠️ tmp_table %s занята другой загрузкой — ждём её окончания", self.tmp_table)
            cur.execute("select pg_advisory_lock(%s)", (lock,))
        return lock

    def _raw_tx(self, cur, conn, key, tbl, raw):
        """Транзакция 1 с tmp_table: проверка её состава до S3, очистка, COPY сырья."""
        self._set_timeout(cur)
        _check_raw(cur, raw, self.tmp_table)
        ctx = self._target(cur, tbl)
        cur.execute("truncate " + raw)
        ctx.raw = raw
        self._fill(cur, conn, ctx, key, tbl)
        return ctx

    def _target_tx(self, cur, ctx, tbl, raw):
        """Транзакция 2 с tmp_table: усечение приёмника, insert из сырья, очистка сырья."""
        self._set_timeout(cur)
        how = _truncate(cur, tbl, self.truncate)
        self.log.info("усечение: %s", how or "нет")
        ctx.loaded += self._insert_from_raw(cur, ctx, tbl, raw)
        cur.execute("truncate " + raw)
        return self._result(ctx)

    def _fill(self, cur, conn, ctx, key, tbl):
        """Файлы объекта S3 по очереди: csv — COPY в приёмник; json/jsonl — в приёмник с одной
        json-колонкой напрямую или в таблицу сырья `ctx.raw` (item jsonb, file_name text):
        tmp_table или временную tmp_stream_raw, она создаётся при первом файле JSON."""
        fcol = ctx.fcol
        fid = _ident(fcol) if fcol else None
        csv_tmp = False
        with self._open() as body:
            for name, chunks in iter_sources(body, key):
                fmt = detect_format(_plain(name), self.format)
                ctx.files.append(name)
                chunks = (c for c in chunks if c)   # пустой кусок s3_IterStream принимает за конец
                if fmt == "csv":
                    if self.tmp_table:
                        raise ValueError(f"{name}: tmp_table — только для json и jsonl")
                    counter = CsvCounter(self.options)
                    stream = (counter.feed(c) for c in _no_bom(chunks))
                    if not fcol:
                        ctx.loaded += self._copy(cur, conn, "copy " + tbl + " from stdin with " + self.options,
                                                 stream, self.options)
                    else:
                        # Колонки файла — все колонки приёмника без fcol: копия без неё, затем insert
                        if not csv_tmp:
                            cur.execute("create temp table tmp_stream_csv (like " + tbl + ") on commit drop" + _dist(cur))
                            cur.execute("alter table tmp_stream_csv drop column " + fid)
                            csv_tmp = True
                        self._copy(cur, conn, "copy tmp_stream_csv from stdin with " + self.options,
                                   stream, self.options)
                        cols = ", ".join(_ident(c.name) for c in ctx.data)
                        cur.execute(f"insert into {tbl} ({cols}, {fid}) select *, %s from tmp_stream_csv", (name,))
                        ctx.loaded += cur.rowcount
                        cur.execute("truncate tmp_stream_csv")
                    ctx.rows += counter.rows()
                    ctx.csv_source = ctx.csv_source or counter.source()
                    continue

                n0 = ctx.feed.n
                if ctx.one_json and ctx.raw is None:     # одна json-колонка, без tmp_table: прямо в приёмник
                    tail = _tail(name) if fcol else b"\n"
                    cols = ", ".join(map(_ident, [ctx.data[0].name] + ([fcol] if fcol else [])))
                    copy = _JSON_COPY.format(tbl, cols)
                    ctx.loaded += self._copy(cur, conn, copy, _batched(ctx.feed.lines(fmt, _text(chunks), tail)))
                else:
                    if ctx.raw is None:
                        cur.execute("create temp table tmp_stream_raw (item jsonb, file_name text) on commit drop"
                                    + _dist(cur))
                        ctx.raw = "tmp_stream_raw"
                    copy = _JSON_COPY.format(ctx.raw, "item, file_name")
                    self._copy(cur, conn, copy, _batched(ctx.feed.lines(fmt, _text(chunks), _tail(name))))
                ctx.rows += ctx.feed.n - n0
        if not ctx.files:
            self.log.warning("⚠️ в архиве %s нет файлов — загружать нечего", key)

    def _insert_from_raw(self, cur, ctx, tbl, raw):
        """insert в приёмник из сырья: объект целиком в json-колонку или раскладка jsonb_populate_record."""
        fcol = ctx.fcol
        if ctx.one_json:
            col = ctx.data[0]
            cols = [col.name]
            sel = ["t.item::json" if col.type_code == 114 else "t.item"]
            src = raw + " t"
        else:
            km = ctx.km
            cols = [c.name for c in ctx.data if c.name in km.seen]
            if not cols and ctx.feed.n:
                raise ValueError(f"{self.s3_key}: ни один ключ не совпал с колонками {self.table}")
            sel = ["r." + _ident(c) for c in cols]
            src = f"{raw} t, jsonb_populate_record(null::{tbl}, t.item) r"
            if km.extra:
                extra = sorted(km.extra)
                self.log.warning("⚠️ ключи без колонки в %s отброшены (%s): %s", self.table, len(extra),
                                 ", ".join(extra[:50]) + (" …" if len(extra) > 50 else ""))
        if fcol:
            cols, sel = cols + [fcol], sel + ["t.file_name"]
        if not cols:
            return 0
        cur.execute(f"insert into {tbl} ({', '.join(map(_ident, cols))}) select {', '.join(sel)} from {src}")
        return cur.rowcount

    def _result(self, ctx):
        km = ctx.km
        out = {
            "rows_source": ctx.rows, "rows_loaded": ctx.loaded, "rows_rejected": ctx.rows - ctx.loaded,
            "files": ctx.files, "target": ctx.target,
            "source": {**ctx.csv_source, **ctx.feed.stats.as_dict()},
            "unmapped": sorted(km.extra) if km else [],
            "defaulted": [c.name for c in ctx.data if c.name not in km.seen] if km and ctx.feed.n else [],
        }
        if ctx.feed.stats.truncated:
            out["source_truncated"] = True
        return out


def _dist(cur):
    """Хвост create table: на Greenplum временные таблицы — distributed randomly."""
    cur.execute("select version()")
    return " distributed randomly" if "greenplum" in cur.fetchone()[0].lower() else ""


def _check_raw(cur, raw, name):
    """tmp_table обязана быть (item jsonb, file_name text) — иначе ошибка до S3."""
    cur.execute("select * from " + raw + " limit 0")
    got = [(c.name, c.type_code) for c in cur.description]
    if got != [("item", 3802), ("file_name", 25)]:
        raise ValueError(f"tmp_table {name}: ждём (item jsonb, file_name text)")


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
    for kw in ({"format": "csv", "tmp_table": "s.raw"}, {"format": "json", "tmp_table": "a;b"}):
        try:
            HrpS3ToPgStreamOperator(task_id="t", s3_key="k", table="t", **kw)
        except ValueError:
            pass
        else:
            raise AssertionError(kw)

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
    text = json.dumps(data)      # \uXXXX на границе куска — не «битый элемент»
    for n in (1, 2, 5, 7):
        assert list(iter_json_array(text[i:i + n] for i in range(0, len(text), n))) == data, n
    try:
        list(iter_json_array(['[{"a":1 x}, ' + '{"b": 2}, ' * 10, "never"]))
    except ValueError as e:
        assert "битый элемент" in str(e), e     # сразу, а не «файл оборван» в конце
    else:
        raise AssertionError("битый элемент в середине")
    assert list(iter_json_array(_text([b"\xef\xbb\xbf[1]"]))) == [1]
    assert list(_no_bom([b"\xef", b"\xbb", b"\xbfid\n", b"1\n"])) == [b"id\n", b"1\n"]
    assert list(_no_bom([b"\xef\xbb\xbf"])) == [] and list(_no_bom([b"a", b"b"])) == [b"ab"]
    old = MAX_ITEM
    globals()["MAX_ITEM"] = 10
    try:
        list(iter_jsonl(["1\n", "2" * 6, "2" * 6]))
    except ValueError as e:
        assert "строка 2" in str(e), e
    else:
        raise AssertionError("длинная строка jsonl")
    finally:
        globals()["MAX_ITEM"] = old
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
    feed = _JsonFeed("data", ["payload"])
    out = list(feed.lines("json", ['{"data": [{"schema": 1, "payload": {"x": "я"}}, {"payload": {"x": null}}]}']))
    assert [json.loads(x) for x in out] == [{"x": "я"}, {"x": None}] and feed.n == 2
    assert feed.stats.as_dict() == {"x": "string|null"}, feed.stats.as_dict()

    km = KeyMap(["employeeid", "name", "src"])
    assert km.map({"employeeId": 1, "extra": 2}, 1) == {"employeeid": 1}
    assert km.extra == {"extra"} and km.seen == {"employeeid"}
    for obj in ({"a": 1, "A": 2}, {"Name": 1, "name": 2}):
        try:
            km.map(obj, 5)
        except ValueError as e:
            assert "элемент 5" in str(e), e
        else:
            raise AssertionError(obj)
    feed = _JsonFeed("", [], KeyMap(["id", "name"]))
    out = list(feed.lines("jsonl", ['{"ID": 1, "x": 2}\n'], _tail("k/a.jsonl")))
    assert out == [b'{"id": 1}\x02k/a.jsonl\n'], out
    assert feed.stats.as_dict() == {"ID": "number", "x": "number"} and feed.km.extra == {"x"}
    try:
        list(feed.lines("jsonl", ["[1]\n"]))
    except ValueError as e:
        assert "элемент 2" in str(e), e
    else:
        raise AssertionError("не объект при раскладке")
    try:
        _tail("a\nb")
    except ValueError:
        pass
    else:
        raise AssertionError("перевод строки в имени")
    assert _plain("x/y.json.GZ") == "x/y.json" and _plain("y.zip") == "y"
    srcs = list(iter_sources(io.BytesIO(gzip.compress(b"x" * (3 * CHUNK))), "k.csv.gz"))
    assert [(n, b"".join(c)) for n, c in srcs] == [("k.csv.gz", b"x" * (3 * CHUNK))]
    assert [(n, b"".join(c)) for n, c in iter_sources(io.BytesIO(b"ab"), "k.csv")] == [("k.csv", b"ab")]
    import zipfile
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("d/", b"")
        z.writestr("d/а.csv", b"1\n")
        z.writestr("b.jsonl", b"{}\n")
    zbuf.seek(0)
    assert [(n, b"".join(c)) for n, c in iter_sources(zbuf, "k.zip")] == [("k.zip/d/а.csv", b"1\n"),
                                                                         ("k.zip/b.jsonl", b"{}\n")]
    print("selfcheck ok")


if __name__ == "__main__":
    _selfcheck()
