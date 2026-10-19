# План: оператор HrpS3ToPgStreamOperator (S3 → GP/PG, CSV и JSON)

> Исполняется скиллом execute: исполнитель на задачу; задача видит только собственный текст.
> Шаги — чекбоксы `- [ ]`. Исполнитель этой фичи — диспетчер сам (код пишет сам, ревью — двумя
> независимыми ревьюерами), поэтому задачи последовательны: все они правят один модуль.

**Спека:** docs/sberpowers/specs/2026-10-15-s3-pg-stream-operator.md (REQ-plugins-07 … REQ-plugins-28)
**Цель:** даг грузит объект S3 (csv, json-массив, jsonl; как есть, `.gz`, `.zip`) в таблицу GP или
PG оператором `HrpS3ToPgStreamOperator` — потоково, одной транзакцией, с усечением `soft/hard/auto`
и раскладкой JSON по колонкам приёмника.
**Архитектура:** один модуль `plugins/pg_stream.py`: чистые функции разбора (stdlib) + оператор на
`BaseOperator`. Байты из S3 → (gzip | stream_unzip) → строки COPY → `s3_IterStream` (из
`plugins/s3_utils`) → одна `copy_expert` на файл. JSON-раскладка — COPY во временную таблицу
`(item jsonb, file_name text)` и `insert … select … from tmp, jsonb_populate_record(null::<приёмник>, tmp.item) r`.
Проверка — самопроверка модуля (`python3 plugins/pg_stream.py`, на стенде) и даг
`tools_test_pg_stream`, который гоняет оператор по сценариям спеки на `cap_gp_etl` (PG16 стенда)
и `gp6-stand` (Greenplum 6.25.4 стенда).

## Глобальные ограничения

- Целевые базы — PostgreSQL 15 и Greenplum 6.25 (PG 9.4): синтаксис новее не использовать; стенд — PG16
  (`cap_gp_etl`) и GP 6.25.4 (`gp6-stand`).
- Python 3.10–3.12, Airflow 2.11.2, psycopg2; новых зависимостей нет (`stream_unzip` уже
  импортирует `plugins/s3_utils`, он есть на обоих контурах).
- Двойной импорт: `from plugins.s3_utils import …` с fallback `from CI06932748.tools.s3_utils import …`.
- Язык кода, комментариев, документации — русский.
- Строка версии — вторая строка docstring модуля и `.md`:
  `*ГГГГ-ММ-ДД ЧЧ:ММ MSK · vX.Y · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*`,
  время — `TZ=Europe/Moscow date '+%Y-%m-%d %H:%M'`; поднимать при каждой правке, кроме одних комментариев.
- Даг: `description=`, короткий `doc_md`, параметры — `Param(description=…)`.
- Импорт модуля не ходит в сеть и в базу; подключения — в `execute`.
- Идентификаторы в SQL — только через `psycopg2.sql.Identifier`; имя таблицы до того проверяется
  регулярным выражением `^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$`.
- `ruff check plugins/pg_stream.py tools/test_pg_stream.py` — чисто до коммита.
- Файлы уезжают в корп (`plugins/**`, `tools/**`): без упоминаний внешних сервисов, агентов, номеров PR.
- Контракт оператора (из спеки):
  `HrpS3ToPgStreamOperator(s3_key, table, bucket=None, s3_conn_id='s3', conn_id='cap_gp_etl',
  format='auto', root=None, item=None, options="CSV HEADER DELIMITER ','", truncate=None,
  file_name_column=None, statement_timeout=240, tmp_table=None, **kwargs)`; `template_fields = ('s3_key', 'table', 'bucket')`.

## Волны

- Волна 1: задача 1.1 — без блокеров.
- Волна 2: задача 2.1 (blocked by 1.1).
- Волна 3: задача 3.1 (blocked by 2.1).
- Волна 3б: задача 3.2 (blocked by 3.1).
- Волна 4: задача 4.1 (blocked by 1.1, 2.1, 3.1, 3.2).
- Волна 5: задача 5.1 (blocked by 4.1).

Задачи последовательны: все правят `plugins/pg_stream.py` и `tools/test_pg_stream.py`.

---

### Задача 1.1: CSV из S3 в таблицу одной транзакцией, усечение, таймаут (волна 1)

**REQ:** REQ-plugins-07, 08, 10, 11, 12, 13, 14, 15, 16, 17 (без zip/gz), 19, 27 (для csv — все колонки из файла)
**model-hint:** frontier
**Blocked by:** нет блокеров

**Контекст:** каркас оператора и самый простой срез — CSV без сжатия прямо в приёмник. На нём
закрепляются транзакция, усечение, таймаут, число строк; JSON и сжатие встают в те же места.

**Файлы:**
- Создать: `plugins/pg_stream.py`
- Создать: `tools/test_pg_stream.py` (даг стенда)

**Интерфейсы:**
- Потребляет: `s3_IterStream(iterator)` — `plugins/s3_utils` (генератор байтов → объект с `read()`).
- Производит:
  - `TRUNCATE_MODES = (None, 'soft', 'hard', 'auto')`
  - `parse_table(table: str) -> psycopg2.sql.Composed` — ValueError на недопустимое имя;
  - `detect_format(name: str, fmt: str) -> str` — `'csv' | 'json' | 'jsonl'`; `name` без `.gz`/`.zip`;
    ValueError «формат не определён по имени <name>»;
  - `_truncate(cur, tbl, mode) -> str | None` — возвращает `'truncate' | 'delete' | None`;
  - `class HrpS3ToPgStreamOperator(BaseOperator)` с `execute(context) -> dict` (итог REQ-11:
    `rows_source`, `rows_loaded`, `rows_rejected`, `files`, `target`, `source`, `unmapped`, `defaulted`[, `source_truncated`]);
  - `_target_types(cur, description) -> dict[str, str]` — `{колонка: typname}`:
    `select oid, typname::text from pg_catalog.pg_type where oid = any(%s)` по `type_code` из
    `description` (имена как у `srv_wf.pr_get_format_type`: `int4`, `varchar`, `timestamp`).

**Шаги:**
- [x] Самопроверка (в `if __name__ == "__main__":` модуля) — пишется первой:
```python
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
    try:
        HrpS3ToPgStreamOperator(task_id="t", s3_key="k", table="t", truncate="yes")
    except ValueError:
        pass
    else:
        raise AssertionError("truncate=yes")
    print("selfcheck ok")
```
- [x] Запусти на стенде `scp` модуля, затем
  `ssh testsrv 'cd /opt/aftest/home/dags && /opt/aftest/venv/bin/python plugins/pg_stream.py' </dev/null`
  — ожидай FAIL (`NameError` / нет модуля).
- [x] Реализуй:
```python
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$")
_EXT = {".csv": "csv", ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl"}

def parse_table(table):
    if not _IDENT.match(table or ""):
        raise ValueError(f"недопустимое имя таблицы: {table!r}")
    return sql.SQL(".").join(sql.Identifier(p) for p in table.split("."))

def detect_format(name, fmt):
    if fmt != "auto":
        if fmt not in ("csv", "json", "jsonl"):
            raise ValueError(f"format={fmt!r}: ждём csv, json, jsonl или auto")
        return fmt
    ext = os.path.splitext(name.lower())[1]
    if ext not in _EXT:
        raise ValueError(f"формат не определён по имени {name}: задайте format")
    return _EXT[ext]

def _truncate(cur, tbl, mode):
    if mode is None:
        return None
    if mode == "hard":
        cur.execute(sql.SQL("truncate {}").format(tbl)); return "truncate"
    if mode == "soft":
        cur.execute(sql.SQL("delete from {}").format(tbl)); return "delete"
    cur.execute("savepoint pg_stream_trunc")            # auto: как pr_soft_truncate
    try:
        cur.execute(sql.SQL("lock table {} in access exclusive mode nowait").format(tbl))
        cur.execute(sql.SQL("truncate {}").format(tbl))
        cur.execute("release savepoint pg_stream_trunc"); return "truncate"
    except psycopg2.errors.LockNotAvailable:
        cur.execute("rollback to savepoint pg_stream_trunc")
        cur.execute(sql.SQL("delete from {}").format(tbl)); return "delete"
```
  Оператор: в `__init__` — `if truncate not in TRUNCATE_MODES: raise ValueError(...)` (до S3 и базы,
  REQ-16). `execute`: `PostgresHook(postgres_conn_id=conn_id).get_conn()` под `closing`,
  `conn.autocommit = False`; `cur.execute("set local statement_timeout = %s", (f"{int(statement_timeout)}min",))`
  при `statement_timeout` > 0, иначе `'0'`; лог таймаута; `_truncate` → лог способа; объект S3 —
  `S3Hook(aws_conn_id=s3_conn_id).get_key(s3_key, bucket)`, нет — `AirflowFailException`; тело под
  `closing`; csv: `cur.copy_expert(sql.SQL("copy {} from stdin with " + options).format(tbl).as_string(conn),
  s3_IterStream(body.iter_chunks(1 << 20)))`; `rows_loaded += cur.rowcount`; `conn.commit()`;
  после каждой COPY — `conn.notices` в лог (и `del conn.notices[:]`); `rows_source` для CSV — поток байт
  идёт в COPY без изменений, а копия проходит `csv.reader` (через инкрементальный декодер utf-8 и
  разбиение по `\n`, чтобы reader сам склеил записи с переводом строки в кавычках) с `delimiter`/`quotechar`
  из `options` (регулярки `DELIMITER\s+'(.)'`, `QUOTE\s+'(.)'`, по умолчанию `,` и `"`), минус 1 при
  `HEADER`; `rows_rejected = rows_source - rows_loaded`, > 0 — `log.warning`; лог «прочитано N, загружено M»;
  `return` словарь: `rows_source`, `rows_loaded`, `rows_rejected`, `files=[s3_key]`, `target=_target_types(...)`, `source` —
  для CSV с `HEADER` в `options` первая строка файла (разбор `csv.reader` по разделителю из `options`,
  по умолчанию `,`) → `{имя: "text"}`, иначе `{}`; `unmapped=[]`, `defaulted=[]`. Любое исключение — `conn.rollback()` (даёт `closing` + явный
  rollback в `except`), исключение пробрасывается.
- [x] Даг `tools/test_pg_stream.py` (`tools_test_pg_stream`, schedule=None, теги `DataTools`, `tools`,
  `AutoQA`; параметры `conn_id` (по умолчанию `cap_gp_etl`, examples `gp6-stand`), `bucket`
  (по умолчанию `edpetl-ctl`), `prefix` (`test/pg_stream/`), `schema` (`public`)). Таск `scenarios`:
  кладёт фикстуры в S3 (`S3Hook.load_bytes`), создаёт таблицы `<schema>.pg_stream_t_*` (на GP —
  `distributed randomly`), зовёт `HrpS3ToPgStreamOperator(task_id=…, …).execute(context)` и
  проверяет `assert`-ами; в `finally` — `drop table if exists`. Сценарии задачи 1.1:
  - CSV 3 строки, `truncate=None` дважды → 6 строк (REQ-12), `rows_source` = `rows_loaded` = 3, `rows_rejected` = 0, `target` = `{"id": "int4", "name": "text"}`, `source` из заголовка (REQ-11);
  - `truncate='hard'` → 3 (REQ-13); `'soft'` → 3, параллельное чтение другим соединением во время
    загрузки не ждёт (REQ-14: второе соединение с `statement_timeout=2s` читает таблицу между
    `_truncate` и `commit` — для этого оператор зовётся с CSV на 200 000 строк, а чтение — в потоке
    `threading.Thread`);
  - `'auto'` без конкурентов → в логе `truncate`; при открытом во втором соединении
    `begin; lock table … in access share mode` → в логе `delete`, оператор не ждёт (REQ-15);
  - битая строка CSV на 1000-й строке при `truncate='hard'` → исключение, в таблице прежние 5 строк (REQ-08);
  - `options="CSV HEADER DELIMITER ';'"` (REQ-19); `truncate='yes'` → ValueError в конструкторе (REQ-16);
  - `.txt` без `format` → ValueError с именем ключа (REQ-17);
  - лог содержит `statement_timeout 240min`, с параметром `5` — `5min` (REQ-10).
  - CSV с переводом строки внутри кавычек → `rows_source` = числу записей (REQ-11);
  - только на GP (`select version()` содержит `Greenplum`): 5 записей, одна с битой датой,
    `options="CSV HEADER LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS"` → `rows_source` 5, `rows_loaded` 4,
    `rows_rejected` 1 (REQ-11); на PG сценарий пропускается с записью в лог.
- [x] Выложи (`scp` + md5) и запусти даг на `cap_gp_etl` и на `gp6-stand`
  (`airflow dags trigger tools_test_pg_stream -c '{"conn_id": "gp6-stand"}'`), жди в фоне.
  Ожидай: оба рана `success`, в логе «selfcheck ok» не нужен — сценарии.
- [x] `ruff check plugins/pg_stream.py tools/test_pg_stream.py` — чисто.
- [x] Commit: `feat(plugins): HrpS3ToPgStreamOperator — CSV одной транзакцией, усечение soft/hard/auto (REQ-plugins-07…19)`

**Критерии выхода:**
- Самопроверка на стенде печатает `selfcheck ok`.
- `tools_test_pg_stream` зелёный на `cap_gp_etl` и `gp6-stand`.

---

### Задача 2.1: JSON-массив, JSON Lines, `item`, приёмник `json`/`jsonb` (волна 2)

**REQ:** REQ-plugins-20, 21, 22, 23 (ветка одной колонки), 09, 11 (`source` для JSON)
**model-hint:** frontier
**Blocked by:** задача 1.1

**Контекст:** потоковый разбор массива уже проверен в `tools/test_json_pg.py` (`iter_json_array`,
строгий, конверт `root`). Он переезжает в `plugins/pg_stream.py`; `tools/test_json_pg.py`
импортирует его оттуда (двойной импорт), своя копия удаляется.

**Файлы:**
- Изменить: `plugins/pg_stream.py`, `tools/test_json_pg.py` (импорт вместо копии, версия +0.1),
  `tools/test_pg_stream.py`

**Интерфейсы:**
- Потребляет: оператор из 1.1.
- Производит:
  - `iter_json_array(chunks: Iterable[str], root: str = "") -> Iterator[Any]` — перенос как есть;
  - `iter_jsonl(chunks: Iterable[str]) -> Iterator[Any]` — ValueError «строка N: …»;
  - `parse_item(item: str | list | None) -> list[str]` — `"a.b"` → `["a","b"]`, список — как есть,
    `None`/`""` → `[]`;
  - `pick(obj, path: list[str], n: int) -> dict` — ValueError «элемент n: <путь до уровня>: нет ключа k»
    или «… не объект»;
  - `class SourceStats` — `add(obj: dict)` копит `{ключ: set(типов JSON)}` (не больше 500 ключей,
    сверх — флаг `truncated`), `as_dict() -> dict[str, str]` — типы через `|` в порядке
    `string, number, boolean, object, array, null`; тип: `str`→string, `bool`→boolean (до int!),
    `int`/`float`→number, `dict`→object, `list`→array, `None`→null;
  - `_copy_line(obj) -> bytes` — `json.dumps(obj, ensure_ascii=False).encode() + b"\n"
    (управляющие символы JSON экранирует, поэтому `\x01`/`\x02` в строке не встречаются).

**Шаги:**
- [x] В `_selfcheck` добавь:
```python
    assert list(iter_json_array(['[1,', '2]'])) == [1, 2]
    assert list(iter_json_array(['{"data": [{"a":1}]}'], "data")) == [{"a": 1}]
    for bad in ('[1,,2]', '[1] x', '[1,'):
        try:
            list(iter_json_array([bad]))
        except ValueError:
            pass
        else:
            raise AssertionError(bad)
    assert list(iter_jsonl(['{"a":1}\n{"a"', ':2}\n\n'])) == [{"a": 1}, {"a": 2}]
    try:
        list(iter_jsonl(['{"a":1}\n{bad\n']))
    except ValueError as e:
        assert "строка 2" in str(e)
    st = SourceStats()
    st.add({"a": 1, "b": True}); st.add({"a": None, "c": [1]})
    assert st.as_dict() == {"a": "number|null", "b": "boolean", "c": "array"}
    big = SourceStats(); big.add({f"k{i}": 1 for i in range(600)})
    assert len(big.as_dict()) == 500 and big.truncated
    assert parse_item("payload.value") == ["payload", "value"]
    assert parse_item(["p", "a.b"]) == ["p", "a.b"]
    assert pick({"p": {"v": {"x": 1}}}, ["p", "v"], 7) == {"x": 1}
    try:
        pick({"p": {}}, ["p", "nope"], 7)
    except ValueError as e:
        assert "7" in str(e) and "nope" in str(e)
```
- [x] Запусти самопроверку на стенде — ожидай FAIL (`NameError: iter_jsonl`).
- [x] Реализуй `iter_jsonl`:
```python
def iter_jsonl(chunks):
    buf, n = "", 0
    for chunk in chunks:
        buf += chunk
        *lines, buf = buf.split("\n")
        for line in lines:
            n += 1
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"строка {n}: {exc}") from exc
    if buf.strip():
        try:
            yield json.loads(buf)
        except json.JSONDecodeError as exc:
            raise ValueError(f"строка {n + 1}: {exc}") from exc
```
  `pick`: идёт по пути, на каждом уровне `isinstance(cur, dict)` и `k in cur`, иначе ValueError с
  `n` и `".".join(path[:i+1])`. Текст из S3 — инкрементальный декодер utf-8, как `_s3_chunks` в
  `tools/test_json_pg.py`. Состав приёмника: `cur.execute(sql.SQL("select * from {} limit 0").format(tbl))`,
  колонки — `cur.description` (`name`, `type_code`); одна колонка (не считая `file_name_column`) с
  `type_code in (114, 3802)` (json, jsonb) → COPY строк `_copy_line(pick(el, path, n))` прямо в неё
  (каждый объект `pick(...)` — ещё и в `SourceStats.add`, если он dict; итог — `source`,
  `source_truncated` в XCom):
  `copy {tbl} ({col}) from stdin with (format csv, quote e'\x01', delimiter e'\x02')`.
- [x] Сценарии в даге: файл ИФТ-образец (синтетический, той же формы: `{"data": [{"schema": {...},
  "payload": {...}}, ×4]}`) в приёмник `(j jsonb)` с `root='data'`, `item='payload'` → 4 строки, ни у одной
  нет ключа `schema` (REQ-20, 22); без `root` → ValueError; файл `[1,,2]` → ошибка, строк 0 (REQ-20);
  jsonl 3 строки + пустая → 3; битая 2-я → «строка 2» (REQ-21); `item='payload.nope'` → ошибка с
  номером и `nope` (REQ-22); REQ-09 — jsonl.gz на 300 МБ после распаковки (появится в 3.1; в 2.1 —
  jsonl 300 МБ без сжатия): `resource.getrusage(RUSAGE_SELF).ru_maxrss` до и после — прирост < 200 МБ.
- [x] Выложи, прогони даг на обоих подключениях; `python3 tools/test_json_pg.py` на стенде — по-прежнему ok.
- [x] `ruff check` — чисто.
- [x] Commit: `feat(plugins): HrpS3ToPgStreamOperator — JSON-массив, JSON Lines, item (REQ-plugins-09, 20…23)`

**Критерии выхода:**
- Самопроверка `selfcheck ok`; `tools_test_pg_stream` зелёный на обоих подключениях;
  `tools_test_json_pg` самопроверка ok.

---

### Задача 3.1: раскладка по колонкам, сопоставление ключей, служебная колонка, gz/zip (волна 3)

**REQ:** REQ-plugins-18, 23 (ветка раскладки), 24, 25, 26, 27, 11 (`unmapped`, `defaulted`, `files` zip)
**model-hint:** frontier
**Blocked by:** задача 2.1

**Контекст:** приёмник не из одной `json`-колонки — объект раскладывается по колонкам. Ключи
нормализуются в Python (регистр), отсутствующие во всех элементах колонки не попадают в список
`insert`, поэтому получают `default` из DDL (REQ-27), а не `NULL` от `jsonb_populate_record`.

**Файлы:**
- Изменить: `plugins/pg_stream.py`, `tools/test_pg_stream.py`

**Интерфейсы:**
- Потребляет: `pick`, `iter_json_array`, `iter_jsonl`, `_copy_line` — задача 2.1.
- Производит:
  - `class KeyMap` — `__init__(columns: list[str])`, `map(obj: dict, n: int) -> dict` (ключи → имена
    колонок; ValueError «элемент n: ключи a и A различаются только регистром»), `extra: set[str]`
    (ключи без колонки), `seen: set[str]` (колонки, встреченные хоть раз);
  - `iter_sources(body, key: str) -> Iterator[tuple[str, Iterator[bytes]]]` — пары (имя файла, байты):
    `.gz` — `gzip.GzipFile(fileobj=body)` кусками; `.zip` — `stream_unzip(body)`, имя `cp866`,
    каталоги пропускаются, имя → `<key>/<имя в архиве>`; иначе `(key, body.iter_chunks())`.

**Шаги:**
- [x] В `_selfcheck` добавь:
```python
    km = KeyMap(["employeeid", "name", "src"])
    assert km.map({"employeeId": 1, "extra": 2}, 1) == {"employeeid": 1}
    assert km.extra == {"extra"} and km.seen == {"employeeid"}
    try:
        km.map({"a": 1, "A": 2}, 5)
    except ValueError as e:
        assert "5" in str(e)
    import gzip as _gz, io as _io
    srcs = list(iter_sources(_io.BytesIO(_gz.compress(b"x")), "k.csv.gz"))
    assert [(n, b"".join(c)) for n, c in srcs] == [("k.csv.gz", b"x")]
```
- [x] Самопроверка на стенде — ожидай FAIL (`NameError: KeyMap`).
- [x] Реализуй раскладку в `execute`:
  - временная таблица: `create temp table tmp_stream_raw (item jsonb, file_name text) on commit drop`
    (+ ` distributed randomly`, если `select version()` содержит `Greenplum`);
  - COPY строк `_copy_line(km.map(pick(el, path, n), n))[:-1] + b"\x02" + имя_файла + b"\n"` в
    `tmp_stream_raw (item, file_name)` с `(format csv, quote e'\x01', delimiter e'\x02')`;
  - после COPY: `cols = [c for c in columns if c in km.seen]`, плюс `file_name_column` →
```sql
insert into {tbl} ({cols}[, {fcol}])
select r.{c1}, r.{c2}…[, t.file_name] from tmp_stream_raw t, jsonb_populate_record(null::{tbl}, t.item) r
```
    `rows_loaded = cur.rowcount`, `rows_source` — число элементов; `km.extra` непусто → один `log.warning` со списком (до 50 имён).
    XCom: `unmapped = sorted(km.extra)`, `defaulted = [c for c in columns if c not in km.seen and
    c != file_name_column]`, `files` — имена из `iter_sources` (для zip — `<key>/<имя в архиве>`).
  - CSV с `file_name_column`: `create temp table tmp_stream_csv (like {tbl}) on commit drop`
    (+ distributed randomly на GP), `alter table tmp_stream_csv drop column {fcol}`, COPY в неё с
    `options`, затем `insert into {tbl} ({cols без fcol}, {fcol}) select *, %s from tmp_stream_csv`.
  - JSON в одну колонку с `file_name_column`: COPY `({col}, {fcol})` с `\x02`-разделителем.
  - `iter_sources` для `.gz`/`.zip`; формат каждого файла — `detect_format(имя без .gz/.zip, format)`;
    все файлы — в той же транзакции.
- [x] Сценарии в даге: приёмник `(employeeid int, name text, hired date, load_dttm timestamp default now())`:
  ключ `employeeId` → `employeeid`; `extra` → предупреждение в логе, ран зелёный; элемент без `name` →
  `NULL`; `load_dttm` заполнен (REQ-23, 24, 27); `{"a":1,"A":2}` → ошибка (REQ-24); `hired: "31.12.2026"`
  → ошибка базы с `31.12.2026`, строк 0 (REQ-25); `file_name_column='src'` для csv, json, jsonl — у всех
  строк ключ S3 (REQ-26); `.json.gz` и zip с двумя csv → строки обоих, у `src` имя внутри архива (REQ-18);
  REQ-09 на `jsonl.gz` 300 МБ; XCom: `unmapped == ["extra"]`, `"load_dttm" in defaulted`,
  `source["employeeId"] == "number"`, для zip `files` — два имени внутри архива (REQ-11).
- [x] Выложи, прогони даг на `cap_gp_etl` и `gp6-stand`.
- [x] `ruff check` — чисто.
- [x] Commit: `feat(plugins): HrpS3ToPgStreamOperator — раскладка по колонкам, file_name_column, gz/zip (REQ-plugins-18, 23…27)`

**Критерии выхода:**
- `selfcheck ok`; `tools_test_pg_stream` зелёный на обоих подключениях, все сценарии 1.1–3.1.

---

### Задача 3.2: таблица сырья `tmp_table` — две транзакции для json/jsonl (волна 3б)

**REQ:** REQ-plugins-28, 08 (ветка двух транзакций)
**model-hint:** frontier
**Blocked by:** задача 3.1

**Контекст:** без `tmp_table` сырьё идёт во временную `tmp_stream_raw (item jsonb, file_name text) on commit drop`
и всё — одна транзакция. `tmp_table` — постоянная таблица того же состава: COPY из S3 коммитится
отдельно, приёмник меняется второй, короткой транзакцией (только SQL в базе). Временная таблица
транзакцию разделить не может — живёт до commit.

**Файлы:**
- Изменить: `plugins/pg_stream.py`, `tools/test_pg_stream.py`

**Интерфейсы:**
- Потребляет: COPY сырья и `insert … jsonb_populate_record` из 3.1 (вынести в функции с именем
  таблицы сырья параметром: `_copy_raw(cur, raw, …)`, `_insert_from_raw(cur, tbl, raw, …)`).
- Производит: параметр `tmp_table=None`; `_check_raw(cur, raw)` — состав `select * limit 0` =
  `[("item", 3802), ("file_name", 25)]`, иначе ValueError «tmp_table <имя>: ждём (item jsonb, file_name text)».

**Шаги:**
- [x] Самопроверка: `HrpS3ToPgStreamOperator(..., format="csv", tmp_table="x")` → ValueError.
- [x] `__init__`: `tmp_table` с `format='csv'` → ValueError; имя — `parse_table`.
- [x] `execute` при `tmp_table`: проверка формата файла (csv при `auto` → ValueError до S3);
  транзакция 1 — `_check_raw`, `truncate tmp_table`, COPY сырья, commit; транзакция 2 — тот же
  `statement_timeout`, `_truncate(приёмник)`, `_insert_from_raw`, `truncate tmp_table`, commit.
  Ошибка во второй — rollback, сырьё остаётся (лог: «сырьё в <tmp_table>»).
- [x] Сценарии: приёмник `(employeeid int, name text, hired date)`, `tmp_table` создаёт даг:
  jsonl + `truncate='hard'` → строки файла, `tmp_table` пуста; `hired: "31.12.2026"` → ошибка,
  приёмник прежний, в `tmp_table` строки файла; `tmp_table (x int)` → ошибка до S3;
  `format='csv'` + `tmp_table` → ValueError.
- [x] Выложи, прогони даг на `cap_gp_etl` и `gp6-stand`; `ruff check` — чисто.
- [x] Commit: `feat(plugins): HrpS3ToPgStreamOperator — tmp_table, две транзакции (REQ-plugins-28)`

**Критерии выхода:**
- `tools_test_pg_stream` зелёный на обоих подключениях, сценарии 1.1–3.2.

---

### Задача 4.1: документация и ревью (волна 4)

**REQ:** все REQ-plugins-07…28 (сверка)
**model-hint:** any
**Blocked by:** задачи 1.1, 2.1, 3.1, 3.2

**Файлы:**
- Изменить: `plugins/pg_stream.py` (шапка-таблица, как у `s3_utils.py`), `tools/readme.md`
  (раздел `test_pg_stream`, версия +0.1), `docs/sberpowers/specs/2026-10-15-s3-pg-stream-operator.md`
  (Traceability: тесты)

**Шаги:**
- [x] Шапка модуля: назначение, параметры оператора таблицей, пример дага:
```python
HrpS3ToPgStreamOperator(
    task_id="load", conn_id="cap_gp_etl", bucket="tfs-in",
    s3_key="{{ params.key }}", table="s_grnplm_vd_hr_edp_dia.tb_scpl",
    root="data", item="payload", truncate="auto", file_name_column="src_file",
)
```
  и ограничения: на сигме — только после переноса в etl-core и строки в `allowed_operators.txt`;
  CSV без `file_name_column` идёт прямо в приёмник — колонки файла = колонки таблицы.
- [x] Ревью двумя независимыми ревьюерами в фоне; находки — `docs/sberpowers/reviews/2026-10-15-s3-pg-stream-operator.md`.
- [x] Принятые находки — исправить, перепрогнать даг на обоих подключениях.
- [x] Commit: `docs(plugins): HrpS3ToPgStreamOperator — шапка модуля, tools/readme, ревью`

**Критерии выхода:**
- Файл ревью с вердиктами `Axis-A`, `Axis-B`, `Tests: tools_test_pg_stream (cap_gp_etl, gp6-stand) → success`.

### Задача 5.1: перенос в Airflow 3, psycopg 3 (волна 5)

**REQ:** REQ-plugins-29
**model-hint:** any
**Blocked by:** задача 4.1

**Файлы:**
- Изменить: `plugins/pg_stream.py` (импорты `airflow.sdk`, оба драйвера), `tools/test_pg_stream.py`
  (импорты `airflow.sdk`, перехват лога оператора подклассом с `log`), `tools/readme.md`

**Шаги:**
- [x] Импорты Airflow 3; драйвер определяется импортом `psycopg`: COPY — `cursor.copy()` / `copy_expert`,
  NOTICE — `add_notice_handler` / `conn.notices`, SQLSTATE — `diag.sqlstate` / `pgcode`, идентификаторы — строкой
  в кавычках, `statement_timeout` — литералом.
- [x] Стенд AF3 (ядро 3.0.1, psycopg 3): `tools_test_pg_stream` на `cap_gp_etl` и `gp6-stand` → success.
- [x] Ревью правки драйверов двумя моделями и поиском молча проглоченных ошибок.
- [x] ~~Та же правка в Airflow 2~~ — отменено 15.10: AF2 остаётся на psycopg2, правка только в AF3.
- [x] Гейт на альфе (AF3, Greenplum `alpha-adb_dev_comm-read`) — 19.10.2026, ран
  `manual__2026-10-19T11:26:33`: `tools_test_pg_stream` → success, сценариев 23, падений нет. `jsonl.gz` 298 МБ
  (1 040 000 строк) — 49 с, пик RSS 425 МБ, прирост 0 МБ; сбой второй транзакции `tmp_table` оставляет сырьё,
  отбраковка COPY 5 → 4 + 1. Шум в логе: `_GrabOp.execute cannot be called outside of the Task Runner!` —
  тест зовёт `execute()` внутри таска; предупреждение OpenTelemetry про `engine` — ядро до 3.0.2.

**Критерии выхода:**
- `tools_test_pg_stream` success на AF3: оба подключения стенда и Greenplum альфы.
