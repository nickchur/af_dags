"""### 🔎 Проверка правила адреса подключения
*2026-10-07 13:53 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

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
