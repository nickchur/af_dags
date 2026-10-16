"""Сведения о системах в `tools_test_connections`: тип/версия PG и слияние `conn_facts`/`ch_clusters`.

Модуль дага при импорте собирает DAG (Variable, пулы), поэтому из него берутся только две чистые
функции — через ast, без исполнения остального.

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "tools" / "test_connections.py"
_tree = ast.parse(_SRC.read_text(encoding="utf-8"))
_ns: dict = {}
exec(compile(ast.Module([n for n in _tree.body if isinstance(n, ast.FunctionDef)
                         and n.name in ("_pg_facts", "_merge_facts")], []), str(_SRC), "exec"), _ns)
_pg_facts, _merge_facts = _ns["_pg_facts"], _ns["_merge_facts"]

GP6 = ("PostgreSQL 9.4.26 (Greenplum Database 6.25.4 build commit:abc) on x86_64-unknown-linux-gnu, "
       "compiled by gcc (GCC) 6.4.0, 64-bit compiled on Jan  1 2024")
PG16 = "PostgreSQL 16.4 (Ubuntu 16.4-1.pgdg22.04+1) on x86_64-pc-linux-gnu, compiled by gcc, 64-bit"


def test_pg_facts_kind_and_version():
    assert (_pg_facts({"version": GP6})["kind"], _pg_facts({"version": GP6})["version"]) == ("GP", "6.25.4")
    f = _pg_facts({"version": PG16, "current_user": "u", "inet_server_addr": None})
    assert (f["kind"], f["version"], f["user"], f["host"]) == ("PG", "16.4", "u", "")
    assert _pg_facts({"version": "PostgreSQL 15.5, compiled by"})["version"] == "15.5"


def _ch(ver, clusters, host="h1"):
    return {"kind": "CH", "version": ver, "host": host, "clusters": clusters}


def test_merge_first_run_and_clusters():
    checked = {"pg": {"kind": "PG", "version": "16.4"},
               "c1": _ch("24.3", {"datalab": ["h1", "h2"]}), "c2": _ch("24.3", {"datalab": ["h2", "h1"]}),
               "solo": _ch("23.8", {}, host="x")}
    facts, clusters, changes = _merge_facts({}, {}, checked, set(checked))
    assert facts == checked
    assert clusters["h1,h2"]["conns"] == ["c1", "c2"] and clusters["x"]["hosts"] == ["x"]
    assert all(c.startswith("новое") for c in changes)
    _, loop, _ = _merge_facts({}, {}, {"l": _ch("24", {"default": ["localhost"]}, host="srv")}, {"l"})
    assert list(loop) == ["srv"]


def test_merge_changes_and_keeps_failed():
    old_f = {"pg": {"kind": "PG", "version": "15.5"}, "gone": {"kind": "S3"}, "down": {"kind": "GP", "version": "6.25.3"}}
    old_c = {"h1,h2": {"hosts": ["h1", "h2"], "version": "24.3", "clusters": {"datalab": ["h1", "h2"]}, "conns": ["c1"]}}
    checked = {"pg": {"kind": "PG", "version": "16.4"}, "c1": _ch("24.8", {"datalab": ["h1", "h2"], "x": ["h1"]})}
    facts, clusters, changes = _merge_facts(old_f, old_c, checked, {"pg", "down", "c1"})
    assert facts["down"] == old_f["down"] and "gone" not in facts   # упавшее сохранено, пропавшее убрано
    text = "\n".join(changes)
    assert "пропало: `gone`" in text and "изменилось: `pg` PG 15.5 → PG 16.4" in text
    assert "кластер `x`: состав — → ['h1']" in text and "версия" not in text


def test_merge_node_added_is_composition_not_new_server():
    """Узел в кластере меняет отпечаток — это «состав», а не «другой сервер»."""
    old_c = {"h1,h2": {"hosts": ["h1", "h2"], "version": "24.3", "clusters": {"datalab": ["h1", "h2"]},
                       "conns": ["c1", "c2"]}}
    checked = {"c1": _ch("24.3", {"datalab": ["h1", "h2", "h3"]})}
    _, clusters, changes = _merge_facts({}, old_c, checked, {"c1", "c2"})
    assert list(clusters) == ["h1,h2,h3"]
    assert [c for c in changes if not c.startswith("новое")] == [
        "кластер `datalab`: состав ['h1', 'h2'] → ['h1', 'h2', 'h3']"]


def test_merge_drops_cluster_without_connections():
    old_c = {"z": {"hosts": ["z"], "version": "1", "clusters": {}, "conns": ["gone"]}}
    assert _merge_facts({}, old_c, {}, set())[1] == {}


def test_merge_keeps_unchecked_cluster():
    old_c = {"z": {"hosts": ["z"], "version": "1", "clusters": {}, "conns": ["cz"]}}
    _, clusters, changes = _merge_facts({}, old_c, {}, {"cz"})
    assert clusters == old_c and not changes


def test_merge_unchecked_conn_stays_in_cluster():
    """c2 того же сервера упал в этот ран — из conns не выпадает; пропавшее из collect — выпадает."""
    old_c = {"h1,h2": {"hosts": ["h1", "h2"], "version": "24.3", "clusters": None, "conns": ["c1", "c2", "c3"]}}
    _, clusters, _ = _merge_facts({}, old_c, {"c1": _ch("24.3", {"datalab": ["h1", "h2"]})}, {"c1", "c2"})
    assert clusters["h1,h2"]["conns"] == ["c1", "c2"]
