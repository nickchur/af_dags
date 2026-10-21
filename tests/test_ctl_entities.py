"""Поддерево сущностей из child?direct=false и адресная проверка (REQ-ctl-worker-39/35).

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

from types import SimpleNamespace

import pytest
import requests
from airflow.sdk.exceptions import AirflowSkipException

from ctl_worker import ctl_utils


def node(eid, *kids, parent=0):
    return {'entity': {'id': eid, 'name': f'e{eid}', 'parentId': parent}, 'kidz': list(kids)}


TREE = node(10, node(30, node(31, parent=30), parent=10), node(20, parent=10))


def test_tree_bfs_sorted_with_root():
    out = ctl_utils.ctl_tree_entities(TREE, 10)
    assert list(out) == [10, 20, 30, 31]
    assert out[31] == {'id': 31, 'name': 'e31', 'parentId': 30}


def test_tree_order_independent_of_response():
    shuffled = node(10, node(20, parent=10), node(30, node(31, parent=30), parent=10))
    assert list(ctl_utils.ctl_tree_entities(shuffled, 10)) == list(ctl_utils.ctl_tree_entities(TREE, 10))


@pytest.mark.parametrize('tree', [None, [], {}, {'entity': {'id': 99}, 'kidz': []}])
def test_tree_without_root_is_empty(tree):
    assert ctl_utils.ctl_tree_entities(tree, 10) == {}


@pytest.fixture
def ctl(monkeypatch):
    calls = []
    answers = {}

    def fake(url, **kw):
        calls.append((url, kw))
        a = answers.get(url, {'id': 1})
        if isinstance(a, BaseException):
            raise a
        return a

    monkeypatch.setattr(ctl_utils, 'ctl_api', fake)
    return SimpleNamespace(calls=calls, answers=answers)


def test_exists_true_without_gp_log(ctl):
    assert ctl_utils.ctl_entity_exists(5) is True
    assert ctl.calls == [('/v4/api/entity/5', {'gp_log': False})]


def test_exists_false_on_4xx(ctl):
    ctl.answers['/v4/api/entity/5'] = AirflowSkipException('404')
    assert ctl_utils.ctl_entity_exists(5) is False


def test_exists_none_on_failure(ctl):
    ctl.answers['/v4/api/entity/5'] = requests.exceptions.ReadTimeout('t')
    assert ctl_utils.ctl_entity_exists(5) is None


def test_present_known_without_requests(ctl):
    assert ctl_utils.ctl_entities_present({1, 2}, {1, 2, 3}) == {1, 2, 3}
    assert ctl.calls == []


def test_present_outside_once_and_by_answer(ctl):
    ctl.answers['/v4/api/entity/7'] = AirflowSkipException('404')
    ctl.answers['/v4/api/entity/8'] = requests.exceptions.ConnectionError('c')
    got = ctl_utils.ctl_entities_present([1, 6, 6, 7, 8], {1})
    assert got == {1, 6, 8}            # 7 — нет в CTL; 8 — сбой, висячей не считаем
    assert [u for u, _ in ctl.calls] == ['/v4/api/entity/6', '/v4/api/entity/7', '/v4/api/entity/8']


def test_ctl_api_maps_404_to_skip(monkeypatch):
    """Настоящий ctl_api: 404 у CTL → AirflowSkipException → ctl_entity_exists() is False."""
    resp = SimpleNamespace(status_code=404, text='404 Entity not found')

    class Hook:
        def __init__(self, **kw):
            pass

        def run(self, **kw):
            raise requests.exceptions.HTTPError(response=resp)

    monkeypatch.setattr(ctl_utils, 'KerberosHttpHook', Hook)
    monkeypatch.setattr(ctl_utils, 'rate_limit', lambda: None)
    monkeypatch.setattr(ctl_utils, 'get_config', lambda: {})
    monkeypatch.setattr(ctl_utils, 'add_note', lambda *a, **k: None)
    assert ctl_utils.ctl_entity_exists(5) is False
