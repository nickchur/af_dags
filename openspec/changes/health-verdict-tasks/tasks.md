## 1. Общий механизм — `plugins/utils.py`

- [x] 1.1 `push_health(checks, context)` — вердикты в XCom ключом `health`; проверка: `ruff`, вызов в стендовом `python` кладёт XCom
- [x] 1.2 `health_tasks(ttl_sec, skill)` — `health_warn` / `health_errors` (`ALL_DONE`): сбор `health` с прямых upstream (в т. ч. mapped), упавший upstream без вердикта → `error`; `health_errors` пишет `report_health`; skip/✅/❌ по правилам спеки; проверка: `airflow dags test` дага-образца на стенде в трёх исходах
- [x] 1.3 `store_params_task`: сообщение в заметку таска (как было у `log_events`) — для всех дагов; проверка: заметка у `params` при `save_params=True`

## 2. Плагины здоровья

- [x] 2.1 `system_health`: `check` → `params` + `collect` + `report` + `health_*`; убрать `raise` и ручной `xcom_push`; `_prev_import_error_id` читает `collect` или `check`; проверка: стенд — здоровый прогон зелёный, заниженный порог → `health_errors` ❌, `collect` зелёный
- [x] 2.2 `pg_activity`: `report` → `push_health` (`alert` → `error`, иначе `warn`), `is_paused_upon_creation=False`; проверка: стенд, находок нет → оба `health_*` skipped
- [x] 2.3 `log_events`: `report` → `push_health` (выше порога `error`, есть сбои `warn`), `params` через `store_params_task`, `is_paused_upon_creation=False`; проверка: стенд (2 сбоя) → `health_warn` ✅, ран зелёный
- [x] 2.4 `test_connections`: параметр `critical`, словарь `task_id → conn_id`, `summary` → `report` с `connections_critical`/`connections` и колонкой «важное»; убрать `raise`; проверка: стенд — с важным среди упавших ран красный, без — зелёный с ⚠️
- [x] 2.5 `test_dags`: `summary` → `report`, `push_health` (`bad` → `error`, немые сравнения → `warn`), убрать `raise`; проверка: стенд, стабильная сериализация → `health_*` skipped
- [x] 2.6 `queue_analyze`: расписание по умолчанию `'10 9 * * *'` (MSK), `report` → `push_health` не выше `warn`, тег `health` вместо `clean`, `is_paused_upon_creation=False`; проверка: стенд без `purge` — брокер не тронут, `schedule_interval` = `10 6 * * *`
- [x] 2.7 Убрать прямые вызовы `report_health` из c072ffa; `REPORT_TTL_SEC` и `skill` передаются в `health_tasks`; проверка: `grep report_health tools/` — только в `plugins/utils.py`

## 3. Единообразие

- [x] 3.1 Переименования: `paused_runs_cleanup.find`, `show_connections.show_connections` → `collect` (`log_cleanup.layout` оставлен: это действие — бакет и правила, не снятие состояния); `test_hrp_operators.summary` → `report`; проверка: `airflow dags list-tasks` на стенде
- [x] 3.2 `db_cleanup`, `log_cleanup`: копия тела → `store_params_task`; проверка: `params` с `save_params=False` пропущен, чистка идёт
- [x] 3.3 Теги-роли у всех дагов `tools` (снять `check` у плагинов, предметные `conn`/`dag`/`mcp`/`operators`/`dummy`); порядок констант в шапке; строки версии; проверка: `grep tags= tools/*.py`

## 4. Навыки и документация

- [x] 4.1 `tools/skill/tools.md` → индекс; новые `tools-system-health`, `tools-pg-activity`, `tools-log-events`, `tools-test-connections`, `tools-test-dags`, `tools-queue-analyze`, `tools-paused-runs`; проверка: `tools_mcp_skills` на стенде публикует новые имена, MCP стенда их читает
- [x] 4.2 `tools/readme.md`: концепция `health_*`, таблица тегов, имена этапов, исключения без `params`; проверка: ссылки на навыки открываются
- [x] 4.3 etl-core #85: навык `airflow-health` — «лог задачи `check`» → `collect`/`health_errors`; проверка: `grep -n 'задачи .check.' skill/airflow-health.md` пуст

## 5. Сквозная проверка

- [x] 5.1 Стенд: сломать проверку (неверный SQL в `pg_activity` временно) → проверочный таск ❌, `health_errors` ❌ «не выполнился», ран красный, `on_callback` сработал; вернуть
- [x] 5.2 Отчёты в `system_health/checks/` у шести плагинов, `skill` — навык своего дага; `ruff check tools plugins`; `sync_context.py`; коммит в PR #82 и описание PR

## 6. Пульс и сторож плагинов (28.09.2026)

- [x] 6.1 `tools_system_pulse` раз в 5 мин в `system_health.py`: `components`, `celery`, `control`, `metabase`, `delivery`; `collect` с заметкой → `health_*`, срок 15 мин; проверка: `airflow dags test` на стенде
- [x] 6.2 `tools_system_health`: `s3_logs`, `pools`, `parsing`, `runs`, `scheduled` и новая `plugins` (свежесть отчётов остальных плагинов, пауза); проверка: на стенде `plugins` назвал запаузенные плагины
- [x] 6.3 `dag_size` → таск в `tools_test_dags`, `tables` убран (размеры таблиц — `report` в `db_cleanup`); проверка: `airflow tasks test tools_test_dags dag_size`
- [x] 6.4 `prune` убран у `queue_analyze` и `pg_activity`: папки чистит `log_cleanup` общим сроком бакета
- [x] 6.5 Readme, навыки `tools`, `tools-system-health`, `tools-test-dags`; etl-core #85 — `airflow-health`, `docs/MCP.md`
