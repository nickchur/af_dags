"""Колбэк `on_callback`: падение без состояния (Airflow 3, колбэк из dag-processor) — не пустое уведомление.

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

import logging
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugins import utils  # noqa: E402


@pytest.fixture
def run(monkeypatch, caplog):
    notes = []
    monkeypatch.setattr(utils, "add_note", lambda m, c, level, add=True: notes.append((level, m)))

    def _run(state=None, try_number=1, max_tries=0, retries=0):
        ti = types.SimpleNamespace(dag_id="d", task_id="collect", map_index=-1, rendered_map_index="",
                                   try_number=try_number, max_tries=max_tries, state=state,
                                   task=types.SimpleNamespace(retries=retries))
        notes.clear()
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=utils.logger.name):
            utils.on_callback({"task_instance": ti, "dag_run": types.SimpleNamespace(state="running"), "task": ti.task})
        return notes, [r.levelname for r in caplog.records if r.name == utils.logger.name]
    return _run


def test_no_state_last_try_is_failed(run):
    notes, levels = run(try_number=3, max_tries=2, retries=2)
    assert "FAILED" in notes[0][1] and "не стартовала" in notes[0][1]
    assert levels == ["ERROR"] and notes[1][0] == "DAG"


def test_no_state_with_tries_left_is_retry(run):
    notes, levels = run(try_number=1, max_tries=2, retries=2)
    assert "UP_FOR_RETRY" in notes[0][1] and "FAILED" not in notes[0][1]
    assert levels == ["WARNING"] and len(notes) == 1


def test_success_unchanged(run):
    notes, levels = run(state="success")
    assert "SUCCESS" in notes[0][1] and "не стартовала" not in notes[0][1] and levels == ["INFO"]
