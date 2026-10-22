"""Раскладка запуска `tools_s3_to_zip` по архивам: вчера по MSK, имена с номерами, одна метка.

Прогон: `python -m pytest tests -q` в окружении Airflow 2 (`/opt/aftest/venv`).
"""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowFailException

from plugins.s3_utils import s3_path_parse

_SRC = Path(__file__).resolve().parents[1] / "s3_tools" / "s3_to_zip.py"
_tree = ast.parse(_SRC.read_text(encoding="utf-8"))
_ns = {'s3_path_parse': s3_path_parse, 'AirflowFailException': AirflowFailException,
       'datetime': datetime, 'timedelta': timedelta, 'timezone': timezone, 'MSK': timezone(timedelta(hours=3))}
exec(compile(ast.Module([n for n in _tree.body if isinstance(n, ast.FunctionDef) and n.name in ('_jobs', '_manual', '_run_after')], []),
             str(_SRC), "exec"), _ns)
_jobs, _manual, _run_after = _ns['_jobs'], _ns['_manual'], _ns['_run_after']

P = {'src': ['s3://b/airflow-logs/', 's3://c/x/airflow-logs/', 's3://b/ctl/'], 'dst': 's3://b/archive/',
     'date_from': None, 'date_to': None, 'add_ts': True, 'max_size_gb': 1, 'replace': False}
NOW = datetime(2026, 10, 18, 22, 30, tzinfo=timezone.utc)   # 19.10 01:30 MSK


def test_names_ts_and_yesterday_msk():
    j = _jobs(P, NOW)
    assert [x['name'] for x in j] == ['airflow-logs', 'airflow-logs_2', 'ctl']
    assert {x['ts'] for x in j} == {'20261018_223000'}
    assert {(x['date_from'], x['date_to']) for x in j} == {('2026-10-18', '2026-10-18')}   # вчера по MSK
    assert j[0]['max_bytes'] == 1024**3 and j[0]['dst'] == 's3://b/archive/'


def test_explicit_dates_no_ts_and_dst_zip():
    j = _jobs({**P, 'src': ['s3://b/a/'], 'date_from': '2026-10-01', 'add_ts': False, 'dst': 's3://b/a.zip'}, NOW)
    assert (j[0]['date_from'], j[0]['date_to'], j[0]['ts']) == ('2026-10-01', None, None)
    with pytest.raises(AirflowFailException, match='папкой'):
        _jobs({**P, 'dst': 's3://b/a.zip'}, NOW)
    with pytest.raises(AirflowFailException, match='src'):
        _jobs({**P, 'src': ['', ' ']}, NOW)


def test_names_unique_against_numbered_and_str_src():
    j = _jobs({**P, 'src': ['s3://b/logs/', 's3://c/logs/', 's3://d/logs_2/']}, NOW)
    assert [x['name'] for x in j] == ['logs', 'logs_3', 'logs_2']      # номер — до первого свободного
    j = _jobs({**P, 'src': 's3://b/one/'}, NOW)                      # строка из переменной — как список из одного
    assert [x['src'] for x in j] == ['s3://b/one/']
    j = _jobs({**P, 'src': ['s3://b/a/'], 'dst': 's3://b/arc/custom.zip'}, NOW)
    assert j[0]['name'] == 'custom'                                   # подпись экземпляра = имя архива


def test_manual_run_today_msk():
    """Ручной ран без дат — сегодня по MSK: 22:30 UTC 18.10 — это уже 19.10 MSK."""
    j = _jobs(P, NOW, manual=True)
    assert {(x['date_from'], x['date_to']) for x in j} == {('2026-10-19', '2026-10-19')}
    j = _jobs(P, NOW)                                                  # авто-ран в тот же момент — вчера
    assert {(x['date_from'], x['date_to']) for x in j} == {('2026-10-18', '2026-10-18')}
    j = _jobs({**P, 'date_to': '2026-10-10'}, NOW, manual=True)       # задана одна — вторая без границы
    assert (j[0]['date_from'], j[0]['date_to']) == (None, '2026-10-10')


def test_manual_detects_enum_and_str():
    from airflow.utils.types import DagRunType
    assert _manual(DagRunType.MANUAL) and _manual('manual')
    assert not any(_manual(t) for t in (DagRunType.SCHEDULED, DagRunType.BACKFILL_JOB, 'scheduled', None))


def test_run_after_af2():
    """AF2 без run_after: ручной — execution_date, по расписанию — конец интервала (слот)."""
    ex, end = datetime(2026, 10, 18, 22, 30, tzinfo=timezone.utc), datetime(2026, 10, 19, 22, 30, tzinfo=timezone.utc)
    assert _run_after(SimpleNamespace(run_type='manual', execution_date=ex, data_interval_end=end)) == ex
    assert _run_after(SimpleNamespace(run_type='scheduled', execution_date=ex, data_interval_end=end)) == end
