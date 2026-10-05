# Список подключений снимает сама проверка доступности

## Why

Список подключений снимал отдельный даг `tools_show_connections` в 23:00 и клал в Variable
`local_connections`; `tools_test_connections` в 23:15 читал её на разборе файла и строил по
таску на подключение. Два дага держались друг за друга только расписанием: сдвиг одного —
и проверка идёт по устаревшему списку. Новое подключение попадало в проверку после
очередного аудита и перечитывания файла, удалённое — падало до следующего; Variable читалась
при каждом разборе файла.

## What Changes

- `tools_show_connections` удалён; его работу делает первый таск `collect` у
  `tools_test_connections`: снимает подключения из secret backend, пишет заметку со списком и
  Variable `local_connections` (её читают выпадающие списки kafka-подключений `test_kafka` и
  `ctl_tfs`, формат прежний).
- Один mapped-таск `check` раскрывается по списку этого же рана, экземпляр подписан «группа ·
  `conn_id`» (`tfs`, `postgres`, `s3`, `ctl`, `clickhouse`, `kafka`, `trino`, `redis`, `other`).
  Группы TaskGroup'ами не делаются: Airflow 2 не раскрывает `expand` по XCom с ключом.
- Флаги `skip_<группа>` в форме, по одному на группу: подключения группы не проверяются (☮️),
  сохраняются с `save_params`.
- Разбор файла Variable не читает.
- Не снялся список (`collect` упал) — ошибка здоровья: проверять было нечего.

## Impact

- `tools/test_connections.py`, удалён `tools/show_connections.py`; readme, навыки `tools`,
  `tools-test-connections`.
- Сетка теряет историю старых task_id (`tfs.<conn>`, `postgres.<conn>`) — разово.
- На контурах остаётся Variable `tools_show_connections_params` — удалить руками; даг
  `tools_show_connections` уйдёт из списка сам после выкладки (файла нет).
