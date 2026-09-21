"""Сторож `af_meta.trigger_dag_run`: он опирается на внутренний интерфейс Task SDK.

Запуск DAG-а изнутри задачи шлёт супервизору `TriggerDagRun` — то же, что раннер делает для
`TriggerDagRunOperator` (`sdk/execution_time/task_runner.py:_handle_trigger_dag_run`,
Airflow 3.3.1). Интерфейс не публичный, поэтому при обновлении Airflow этот тест должен
первым сказать, что сообщение, канал или коды ошибок изменились.

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

import pytest

from plugins import af_meta


def test_comms_interface_is_where_we_expect():
    """Класс сообщения, его поля и канал супервизора — на месте."""
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import ErrorResponse, TriggerDagRun

    fields = set(TriggerDagRun.model_fields)
    assert {'dag_id', 'run_id', 'conf', 'logical_date', 'run_after', 'reset_dag_run'} <= fields
    # Вне процесса задачи значения нет — только аннотация модуля; присваивает его
    # task_runner.main() (global SUPERVISOR_COMMS), то есть ровно там, где мы его берём.
    assert 'SUPERVISOR_COMMS' in task_runner.__annotations__, "канал супервизора переименован"
    assert ErrorType.DAGRUN_ALREADY_EXISTS
    assert ErrorResponse(error=ErrorType.API_SERVER_ERROR).error is ErrorType.API_SERVER_ERROR
    err = AirflowRuntimeError(error=ErrorResponse(error=ErrorType.API_SERVER_ERROR, detail={'status_code': 404}))
    assert err.error.detail['status_code'] == 404


class _Comms:
    def __init__(self, reply):
        self.reply = reply
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


@pytest.fixture
def comms(monkeypatch):
    from airflow.sdk.execution_time import task_runner

    def install(reply):
        c = _Comms(reply)
        monkeypatch.setattr(task_runner, 'SUPERVISOR_COMMS', c, raising=False)
        return c

    return install


def test_trigger_sends_message_with_logical_date(comms):
    c = comms(None)
    af_meta.trigger_dag_run('CTL.wf', 'sensor__1_1_x', {'loading_id': 1})
    msg = c.sent[0]
    assert (msg.dag_id, msg.run_id, msg.conf) == ('CTL.wf', 'sensor__1_1_x', {'loading_id': 1})
    assert msg.logical_date is not None and msg.logical_date.tzinfo is not None
    assert msg.reset_dag_run is False


def test_409_is_already_exists(comms):
    from airflow.sdk.exceptions import ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    comms(ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS))
    with pytest.raises(af_meta.DagRunAlreadyExists):
        af_meta.trigger_dag_run('CTL.wf', 'r1')


def test_404_is_dag_not_found(comms):
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    comms(AirflowRuntimeError(ErrorResponse(
        error=ErrorType.API_SERVER_ERROR,
        detail={'status_code': 404, 'detail': {'reason': 'not_found'}})))
    with pytest.raises(af_meta.DagNotFound):
        af_meta.trigger_dag_run('CTL.gone', 'r1')


def test_other_api_error_is_not_swallowed(comms):
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    comms(AirflowRuntimeError(ErrorResponse(
        error=ErrorType.API_SERVER_ERROR, detail={'status_code': 400, 'detail': {'reason': 'import_errors'}})))
    with pytest.raises(AirflowRuntimeError):
        af_meta.trigger_dag_run('CTL.broken', 'r1')
