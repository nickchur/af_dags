"""### 🔎 Проверка правила адреса подключения
*2026-10-07 14:32 MSK · v1.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Правило адреса `_conn_addr` и подпись `_label` из `tools/test_connections.py` (REQ-tools-42, REQ-tools-02):
хост из подключения, иначе первый непустой строковый ключ extra (`endpoint_url`, `bootstrap.servers`);
без запятой — хост и порт, с запятой — хосты без портов и порт первого. Непригодный адрес из extra даёт
(None, None) и одно предупреждение в лог без значения. Подпись обрезается до LABEL_MAX (250 символов) с «…».

Модуль дага не импортируется (он требует Airflow и метабазу): функции и константы вынимаются
через `ast` и исполняются отдельно. Запуск из любого места: `python3 testbed/check_conn_addr.py`.
"""
import ast
import sys
from pathlib import Path
from typing import Optional

SRC = Path(__file__).resolve().parent.parent / "tools" / "test_connections.py"


class WarnLogger:
    """Заглушка logger для подсчёта вызовов warning без вывода в stderr."""

    def __init__(self):
        self.warnings: list[tuple[str, tuple]] = []

    def warning(self, msg: str, *args):
        self.warnings.append((msg, args))

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


def load():
    """`_conn_addr`, `_label`, `ADDR_EXTRA_KEYS`, `LABEL_MAX` из модуля дага без его импорта."""
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    keep = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in ("_conn_addr", "_label"))
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in ("ADDR_EXTRA_KEYS", "LABEL_MAX") for t in n.targets))]
    mock_logger = WarnLogger()
    ns = {"Optional": Optional, "logger": mock_logger}
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(SRC), "exec"), ns)
    return ns["_conn_addr"], ns["_label"], ns["LABEL_MAX"], mock_logger


CASES = [
    # (args), want_addr, want_warn
    # Корректные случаи (want_warn=False)
    (("gp.stand", 5432, {}), ("gp.stand", 5432), False),
    (("h1,h2", 5432, {"endpoint_url": "http://s3:9000"}), ("h1,h2", 5432), False),
    ((None, None, {"endpoint_url": "https://s3.example.ru:9443"}), ("s3.example.ru", 9443), False),
    ((None, None, {"endpoint_url": "https://s3.example.ru"}), ("s3.example.ru", None), False),
    ((None, None, {"bootstrap.servers": "k1:9093"}), ("k1", 9093), False),
    ((None, None, {"bootstrap.servers": "k1:9093, k2:9094"}), ("k1,k2", 9093), False),
    ((None, None, {"bootstrap.servers": "k1,k2:9094"}), ("k1,k2", None), False),
    (("", None, {"endpoint_url": "http://s3:9000", "bootstrap.servers": "k1:9093"}), ("s3", 9000), False),
    ((None, None, {"endpoint_url": "", "bootstrap.servers": "k1:9093"}), ("k1", 9093), False),
    ((None, None, {"endpoint_url": "  ", "bootstrap.servers": "k1:9093"}), ("k1", 9093), False),
    ((None, None, {"region_name": "ru", "password": "x"}), (None, None), False),
    ((None, None, {}), (None, None), False),

    # Непригодные адреса из extra (want_warn=True)
    ((None, None, {"bootstrap.servers": "k1:abc"}), (None, None), True),
    ((None, None, {"bootstrap.servers": "k1:99999"}), (None, None), True),
    ((None, None, {"endpoint_url": "http://:9000"}), (None, None), True),
    ((None, None, {"bootstrap.servers": "k1:9093,,k2"}), (None, None), True),
    ((None, None, {"bootstrap.servers": ","}), (None, None), True),
    ((None, None, {"endpoint_url": "http://[::1"}), (None, None), True),
    ((None, None, {"bootstrap.servers": ["k1:9093"]}), (None, None), True),
    ((None, None, {"bootstrap.servers": ["k1:9093"], "endpoint_url": None}), (None, None), True),
    ((None, None, [1]), (None, None), True),
]


def main():
    conn_addr, label, label_max, mock_logger = load()
    bad = 0
    for args, want, want_warn in CASES:
        mock_logger.warnings.clear()
        got = conn_addr(*args)
        warn_count = len(mock_logger.warnings)
        if got != want:
            bad += 1
            print(f"FAIL {args} → {got}, ждали {want}")
        elif want_warn and warn_count != 1:
            bad += 1
            print(f"FAIL {args} → предупреждений {warn_count}, ждали 1")
        elif not want_warn and warn_count != 0:
            bad += 1
            print(f"FAIL {args} → лишнее предупреждение ({warn_count})")

    # Проверка передачи conn_id и формата предупреждения
    mock_logger.warnings.clear()
    conn_addr(None, None, {"bootstrap.servers": "k1:abc"}, conn_id="test_conn")
    if len(mock_logger.warnings) != 1:
        bad += 1
        print(f"FAIL conn_id check → вызовов {len(mock_logger.warnings)}, ждали 1")
    else:
        fmt, fmt_args = mock_logger.warnings[0]
        if fmt_args[0] != "test_conn":
            bad += 1
            print(f"FAIL conn_id check → conn_id={fmt_args[0]!r}, ждали 'test_conn'")

    # Проверка _label и LABEL_MAX: короткая подпись как есть, длинная — ровно 250 символов с «…»
    short_item = {"group": "s3", "conn_id": "s3", "host": None, "port": None}
    short_want = "s3 · s3 · —:—"
    short_got = label(short_item)
    if short_got != short_want:
        bad += 1
        print(f"FAIL label({short_item}) → {short_got!r}, ждали {short_want!r}")

    long_item = {"group": "s3", "conn_id": "s3", "host": "h" * 300, "port": None}
    long_got = label(long_item)
    if len(long_got) != label_max or not long_got.endswith("…"):
        bad += 1
        print(f"FAIL label long host → длина {len(long_got)} (ждали {label_max}), оканчивается на {long_got[-1:]!r}")

    total = len(CASES) + 3
    ok = total - bad
    print(f"{ok}/{total} ok")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
