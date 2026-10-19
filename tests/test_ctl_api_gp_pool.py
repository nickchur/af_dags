"""`ctl_api`: gp_pool читается раз в TTL, сбой метабазы не роняет GET (REQ-ctl-api-01/02).

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

from types import SimpleNamespace

import pytest

from ctl_worker import ctl_utils


@pytest.fixture
def env(monkeypatch):
    calls = SimpleNamespace(pool=0, gp=0, pool_exc=None, slots=5)

    def pool_size(name):
        calls.pool += 1
        if calls.pool_exc:
            raise calls.pool_exc
        return calls.slots

    class Hook:
        def __init__(self, **kw):
            pass

        def run(self, **kw):
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {'ok': 1}, status_code=200)

    now = [1000.0]
    monkeypatch.setattr(ctl_utils, 'pool_size', pool_size)
    monkeypatch.setattr(ctl_utils, 'gp_exe', lambda *a, **k: setattr(calls, 'gp', calls.gp + 1))
    monkeypatch.setattr(ctl_utils, 'KerberosHttpHook', Hook)
    monkeypatch.setattr(ctl_utils, 'rate_limit', lambda: None)
    monkeypatch.setattr(ctl_utils, 'get_config', lambda: {})
    monkeypatch.setattr(ctl_utils.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(ctl_utils, '_gp_pool', {'ts': None, 'open': False})
    calls.notes = []
    monkeypatch.setattr(ctl_utils, 'add_note', lambda msg, **k: calls.notes.append(msg))
    calls.now = now
    return calls


@pytest.mark.parametrize('kw', [
    {'url': '/v5/api/wf/1', 'gp_log': False},
    {'url': '/v5/api/wf/1', 'method': 'POST'},
    {'url': '/v5/api/tmpl/1'},
    {'url': '/v5/api/statval/1'},
])
def test_no_journal_no_metabase(env, kw):
    ctl_utils.ctl_api(**kw)
    assert env.pool == 0 and env.gp == 0


def test_pool_read_once_per_ttl(env):
    for _ in range(100):
        assert ctl_utils.ctl_api('/v5/api/wf/1') == {'ok': 1}
    assert env.pool == 1 and env.gp == 100
    env.now[0] += ctl_utils.GP_POOL_TTL
    ctl_utils.ctl_api('/v5/api/wf/1')
    assert env.pool == 2


def test_closed_pool_skips_journal(env):
    env.slots = 0
    ctl_utils.ctl_api('/v5/api/wf/1')
    assert env.pool == 1 and env.gp == 0


def test_metabase_failure_does_not_fail_get(env, caplog):
    env.pool_exc = TimeoutError('connection timeout expired')
    with caplog.at_level('ERROR'):
        assert ctl_utils.ctl_api('/v5/api/wf/1') == {'ok': 1}
    assert ctl_utils.ctl_api('/v5/api/wf/2') == {'ok': 1}
    assert env.pool == 1 and env.gp == 0  # в пределах TTL метабазу не трогаем
    assert any('gp_pool не прочитан' in r.getMessage() for r in caplog.records)
    assert len(env.notes) == 1 and 'connection timeout expired' in env.notes[0]
    env.pool_exc = None
    env.now[0] += ctl_utils.GP_POOL_TTL
    ctl_utils.ctl_api('/v5/api/wf/3')
    assert env.pool == 2 and env.gp == 1
