# План: хосты и порты подключений в проверке доступности

> Исполняется скиллом execute: свежий исполнитель на задачу; задача видит
> только собственный текст. Шаги — чекбоксы `- [ ]`.

**Спека:** docs/sberpowers/specs/2026-10-07-test-connections-host-port.md (REQ-tools-42, REQ-tools-01, REQ-tools-02)
**Цель:** у каждого подключения в `tools_test_connections` виден адрес `хост:порт` — в подписи
экземпляра `check`, в строке сводки `report`, полной таблицей в логе `collect`, в XCom `collect`
под ключом `connections` и в Variable `local_connections`.
**Архитектура:** правило адреса — одна чистая функция `_conn_addr` в модуле дага (только
stdlib); подпись экземпляра — одна функция `_label`, её зовут и `check`, и `report`. Правило
проверяется скриптом `testbed/check_conn_addr.py`, который вынимает функцию из модуля через `ast`
и гоняет её без Airflow; живой прогон — на тестовом стенде силами диспетчера.

## Глобальные ограничения

- Python 3.10–3.12, Airflow 2.11.2; новых зависимостей нет, в `_conn_addr` — только stdlib.
- Язык кода, комментариев и документации — русский.
- Строка версии — вторая строка docstring модуля и `.md`: `*ГГГГ-ММ-ДД ЧЧ:ММ MSK · vX.Y · Nick
  Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*`; время — `TZ=Europe/Moscow date '+%Y-%m-%d %H:%M'`;
  версия поднимается на минорную ступень при каждой правке файла.
- Окончания строк файла сохраняются (CRLF остаётся CRLF); дифф — только изменённые строки.
- В адрес, лог, XCom и Variable не попадают логин, пароль и параметры extra — только хост и порт.
- XCom `check` не меняется: `{"status", "conn_id", "conn_type"}`.
- Заметка `collect` не меняется: строка на тип с именами, без адресов (заметка ≤ 1000 символов).
- `return_value` `collect` остаётся списком элементов для `check.expand(item=…)`.
- Файлы, уезжающие в корпоративный репозиторий (`tools/**`, `testbed/**`): без упоминаний
  внешних сервисов, агентов, номеров PR.

## Волны

- Волна 1: задача 1.1 — без блокеров.
- После волны: прогон на стенде (диспетчер), см. «Проверка на стенде».

## Задачи

### Задача 1.1: Адрес подключения в подписи, сводке, логе и XCom (волна 1)

**REQ:** REQ-tools-42, REQ-tools-01, REQ-tools-02
**model-hint:** any
**Blocked by:** нет блокеров

**Контекст:** DAG `tools_test_connections` (`tools/test_connections.py`) первым таском `collect`
снимает подключения из secret backend (`backend._local_connections`, словарь `conn_id →
airflow.models.Connection`), пишет Variable `local_connections` и возвращает список проверок;
mapped-таск `check` проверяет по подключению, `report` пишет сводку в заметку рана. Сейчас
адреса видно только в Variable, а у S3 и Kafka `host`/`port` пусты: адрес лежит в extra
(`endpoint_url` у S3, `bootstrap.servers` у Kafka). Задача добавляет правило адреса и выводит
адрес везде, где перечислено в спеке.

**Файлы:**
- Изменить: `tools/test_connections.py` — константа, две функции, таски `collect`, `check`, `report`, docstring
- Создать: `testbed/check_conn_addr.py` — проверка правила адреса
- Изменить: `testbed/README.md` — строка о новом скрипте в таблице скриптов
- Изменить: `tools/readme.md` — раздел `test_connections.py`
- Изменить: `tools/skill/tools-test-connections.md` — абзац «Таски» и абзац о `collect`

**Интерфейсы:**
- Производит (в `tools/test_connections.py`, уровень модуля):
  - `ADDR_EXTRA_KEYS = ("endpoint_url", "bootstrap.servers")`
  - `_conn_addr(host: Optional[str], port: Optional[int], extra: dict) -> tuple[Optional[str], Optional[int]]`
  - `_label(item: dict) -> str` — `"<group> · <conn_id> · <host или —>:<port или —>"`
- Элемент списка проверок (`return_value` `collect`, вход `check`): `{"group", "conn_id",
  "conn_type", "host", "port"}`.
- XCom `collect` с ключом `connections` и Variable `local_connections` — один и тот же словарь
  `{тип: [{"conn_id", "host", "port", "schema", "description"}]}`, тип `sqlite` → `clickhouse`.

**Шаги:**

- [ ] Создай `testbed/check_conn_addr.py` — падающая сначала проверка правила:

```python
"""### 🔎 Проверка правила адреса подключения
*<ГГГГ-ММ-ДД ЧЧ:ММ> MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Правило адреса `_conn_addr` из `tools/test_connections.py` (REQ-tools-42): хост из подключения,
иначе первый непустой ключ extra (`endpoint_url`, `bootstrap.servers`); без запятой — хост и
порт, с запятой — хосты без портов и порт первого.

Модуль дага не импортируется (он требует Airflow и метабазу): функция и константа вынимаются
через `ast` и исполняются отдельно. Запуск из любого места: `python3 testbed/check_conn_addr.py`.
"""
import ast
import sys
from pathlib import Path
from typing import Optional

SRC = Path(__file__).resolve().parent.parent / "tools" / "test_connections.py"


def load():
    """`_conn_addr` и `ADDR_EXTRA_KEYS` из модуля дага, без его импорта."""
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    keep = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name == "_conn_addr")
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "ADDR_EXTRA_KEYS" for t in n.targets))]
    ns = {"Optional": Optional}
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(SRC), "exec"), ns)
    return ns["_conn_addr"]


CASES = [
    # (host, port, extra) → (host, port)
    (("gp.stand", 5432, {}), ("gp.stand", 5432)),
    (("h1,h2", 5432, {"endpoint_url": "http://s3:9000"}), ("h1,h2", 5432)),
    ((None, None, {"endpoint_url": "https://s3.example.ru:9443"}), ("s3.example.ru", 9443)),
    ((None, None, {"endpoint_url": "https://s3.example.ru"}), ("s3.example.ru", None)),
    ((None, None, {"bootstrap.servers": "k1:9093"}), ("k1", 9093)),
    ((None, None, {"bootstrap.servers": "k1:9093, k2:9094"}), ("k1,k2", 9093)),
    ((None, None, {"bootstrap.servers": "k1,k2:9094"}), ("k1,k2", None)),
    (("", None, {"endpoint_url": "http://s3:9000", "bootstrap.servers": "k1:9093"}), ("s3", 9000)),
    ((None, None, {"endpoint_url": "", "bootstrap.servers": "k1:9093"}), ("k1", 9093)),
    ((None, None, {"region_name": "ru", "password": "x"}), (None, None)),
    ((None, None, {}), (None, None)),
    ((None, None, {"bootstrap.servers": "k1:abc"}), ("k1", None)),
]


def main():
    conn_addr = load()
    bad = 0
    for args, want in CASES:
        got = conn_addr(*args)
        if got != want:
            bad += 1
            print(f"FAIL {args} → {got}, ждали {want}")
    print(f"{len(CASES) - bad}/{len(CASES)} ok")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] Запусти `python3 testbed/check_conn_addr.py` — ожидай падение `KeyError: '_conn_addr'`
  (функции в модуле ещё нет).

- [ ] В `tools/test_connections.py` после функции `_group` (раздел «Группы проверок») добавь:

```python
# Ключи extra, где лежит адрес, когда хост подключения пуст: S3 — endpoint_url, Kafka —
# bootstrap.servers (так их кладёт secret backend). Порядок — приоритет
ADDR_EXTRA_KEYS = ("endpoint_url", "bootstrap.servers")


def _conn_addr(host: Optional[str], port: Optional[int], extra: dict) -> tuple[Optional[str], Optional[int]]:
    """Адрес подключения: хост и порт.

    Хост заполнен — как есть. Пуст — первый непустой ключ extra из ADDR_EXTRA_KEYS: без запятой
    раскладывается на хост и порт (`схема://хост:порт` или `хост:порт`), с запятой — хосты без
    портов через запятую и порт первого. Тот же вид `h1,h2` + порт первого secret backend сам
    строит для Postgres с несколькими хостами. Адреса нет — (None, None).
    """
    from urllib.parse import urlsplit

    if host:
        return host, port
    raw = next((str(extra[k]) for k in ADDR_EXTRA_KEYS if extra.get(k)), "")
    if not raw:
        return None, None
    parts = [urlsplit(p if "//" in p else f"//{p}") for p in (s.strip() for s in raw.split(","))]
    try:
        first_port = parts[0].port
    except ValueError:  # порт не число — показываем без порта, а не роняем сбор списка
        first_port = None
    return ",".join(p.hostname or "" for p in parts), first_port


def _label(item: dict) -> str:
    """Подпись экземпляра check и строки сводки: «группа · conn_id · хост:порт»."""
    return f"{item.get('group', '?')} · {item['conn_id']} · {item.get('host') or '—'}:{item.get('port') or '—'}"
```

- [ ] Запусти `python3 testbed/check_conn_addr.py` — ожидай `12/12 ok`, код выхода 0.

- [ ] В таске `collect` замени цикл сборки (сейчас — `for cid, conn in sorted(conns.items()):`
  с `by_type[...].append({... "host": conn.host, "port": conn.port ...})` и
  `items.append({"group": ..., "conn_id": cid, "conn_type": conn.conn_type})`) и запись Variable на:

```python
        by_type = defaultdict(list)
        items = []
        for cid, conn in sorted(conns.items()):
            host, port = _conn_addr(conn.host, conn.port, conn.extra_dejson)
            by_type["clickhouse" if conn.conn_type == "sqlite" else conn.conn_type].append({
                "conn_id": cid, "host": host, "port": port, "schema": conn.schema,
                "description": conn.description or "No description",
            })
            items.append({"group": _group(cid, conn.conn_type), "conn_id": cid, "conn_type": conn.conn_type,
                          "host": host, "port": port})
        Variable.set("local_connections", dict(by_type), serialize_json=True)
        # Тот же словарь — в XCom под своим ключом, как его отдавал прежний аудит подключений;
        # return_value занят списком для expand
        context["ti"].xcom_push(key="connections", value=dict(by_type))

        # Полная таблица — в лог: в заметку (1000 символов) сотня строк не влезает
        headers = ("conn_type", "conn_id", "host", "port", "schema", "description")
        table = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
        table += ["| " + " | ".join(str(c[h] if h != "conn_type" else ctype) for h in headers) + " |"
                  for ctype, rows in sorted(by_type.items()) for c in rows]
        logger.info("Подключения:\n%s", "\n".join(table))
```

  Комментарий над циклом («Variable — для выпадающих списков…») оставь; комментарий «Строка на
  тип, без таблицы: хосты и описания — в Variable local_connections» замени на «Строка на тип,
  без таблицы: адреса — в логе, в подписях check и в report». Строки с `Feed(...)` и `return`
  не меняй.

- [ ] В таске `check` замени строку `context["conn_label"] = f"{item['group']} · {item['conn_id']}"`
  на `context["conn_label"] = _label(item)`, docstring таска — на
  `"""Проверка одного подключения; в сетке экземпляр подписан «группа · conn_id · хост:порт»."""`.
  `return _run_test(...)` не меняй: XCom check остаётся `{status, conn_id, conn_type}`.

- [ ] В таске `report` замени `name = f"{item.get('group', '?')} · {conn_id}"` на
  `name = _label({**item, "conn_id": conn_id})` (у `item` может не быть ключей, если список
  короче: `conn_id` уже подставлен строкой выше).

- [ ] Docstring модуля: подними версию (v3.9 → v3.10) и время; в абзаце о тасках после
  «`collect` снимает список (и обновляет Variable `local_connections`)» допиши «, адреса — в XCom
  `connections` и таблицей в логе», а «mapped `check` — по экземпляру на подключение» дополни
  «, подписан «группа · conn_id · хост:порт»».

- [ ] `tools/readme.md`, раздел `test_connections.py`: подними версию файла; пункт «Список в том
  же ране» — «(заметка — строка на тип; полная таблица `conn_type, conn_id, host, port, schema,
  description` — в логе; тот же словарь, что в Variable, — в XCom `collect` под ключом
  `connections`)»; пункт «Один mapped-таск `check`» — подпись «группа · `conn_id` · хост:порт»
  и новый подпункт «**Адрес**: хост и порт подключения; хост пуст — первый непустой из extra
  `endpoint_url`, `bootstrap.servers`: без запятой — хост и порт, с запятой — хосты без портов и
  порт первого (как secret backend строит Postgres с несколькими хостами). Тот же адрес — в
  строках `report`»; пункт «Отчетность» — «строка … с подписью экземпляра».

- [ ] `tools/skill/tools-test-connections.md`: подними версию; в абзаце о `collect` «(заметка —
  строка на тип; …)» допиши «адреса — в XCom `connections` таска `collect` и таблицей в его
  логе»; в «Таски» подпись «группа · `conn_id` · хост:порт», после неё фраза «— по адресу видно,
  куда не достучались; у S3 и Kafka он из extra (`endpoint_url`, `bootstrap.servers`)».

- [ ] `testbed/README.md`: в таблицу скриптов (рядом со строкой `check_status_contract.py`)
  добавь `| [`check_conn_addr.py`](check_conn_addr.py) | Проверка правила адреса подключения
  `tools_test_connections`: хост из подключения или из extra, разбор списка хостов |`;
  подними версию файла, если у него есть строка версии.

- [ ] Проверь: `python3 testbed/check_conn_addr.py` → `12/12 ok`;
  `ruff check --select F821,F811 tools/test_connections.py testbed/check_conn_addr.py` → `All checks passed!`;
  `git diff --stat` — только пять файлов из списка; окончания строк не изменились (дифф не на весь файл).

- [ ] Commit (делает диспетчер): `feat(tools): test_connections v3.10 — адрес подключения в подписи, сводке, логе и XCom`

**Критерии выхода:**
- `python3 testbed/check_conn_addr.py` — `12/12 ok`, код 0.
- `ruff check --select F821,F811` по двум файлам — без находок.
- В `tools/test_connections.py` есть `ADDR_EXTRA_KEYS`, `_conn_addr`, `_label` с сигнатурами из
  «Интерфейсов»; `check` и `report` подписывают через `_label`; `return` у `check` не изменён.
- Изменены только пять файлов из списка.

## Проверка на стенде (диспетчер, после задачи 1.1)

Выложить `tools/test_connections.py` в `/opt/aftest/home/dags/tools/`, запустить
`tools_test_connections`, по прогону сверить критерии приёмки спеки:
- сетка: подписи `postgres · stand-dwh-read · 127.0.0.1:5432`, у S3 и Kafka — адрес из extra
  (на стенде у части подключений extra без адреса — там `—:—`), экземпляров столько же, сколько
  подключений;
- лог `collect`: таблица со всеми подключениями, без `--- Logging error ---`;
- XCom `collect` ключ `connections` совпадает с Variable `local_connections`; XCom `check` —
  `{status, conn_id, conn_type}`;
- заметка `collect` — строка на тип, без адресов;
- строка падения в `report`: временно сломать вспомогательное подключение нельзя — проверяется
  по ближайшему упавшему или пропущенному экземпляру (☮️ trino на стенде).
