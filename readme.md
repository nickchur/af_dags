# af3_dags — даги HR-платформы под Airflow 3

*2026-09-21 18:44 MSK · v0.3 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Перенос дагов из `af_dags` (Airflow 2) на Airflow 3.3.1 — этап 8 плана миграции
`etl-core-3/openspec/changes/airflow-3-migration/tasks.md`. **Приватный**: сюда можно то, что
в публичный `af_dags` класть нельзя, но пароли и ключи не кладутся и здесь.

## Чем даг AF3 отличается от дага AF2

- Импорты — из `airflow.sdk` (`DAG`, `task`, `Param`, `Variable`, `Asset`, `AssetAlias`,
  `PokeReturnValue`, `TaskInstanceState`, исключения — `airflow.sdk.exceptions`).
  `Variable.get(..., default=...)`, а не `default_var=`. `schedule=`, а не `schedule_interval=`.
- **У задачи и у разбора DAG-файла нет ORM-доступа к метабазе.** Всё, что шло через
  `create_session`, `DagModel`, `TaskInstance`, `Pool`, `Log`, заметки и `trigger_dag`, идёт
  через `plugins/af_meta.py` (см. ниже).
- `Dataset*` → `Asset*`: `triggering_asset_events` (ключ — объект `Asset`, у события
  `source_run_id`), `outlet_events[AssetAlias(...)]` — ключ объект, не строка; сам
  `outlet_events` ленивый и до первой записи пуст.
- **`xcom_pull` маппленной задачи** (`task_ids` строкой): при единственной найденной копии
  отдаёт само значение, а не список. Списком `task_ids=[...]` ответ всегда список — но с
  `None` на месте копий без результата.
- В метабазе: `execution_date` → `logical_date` (может быть пуст, момент рана — `run_after`),
  `is_active` → `is_stale` (смысл обратный), `task_instance.job_id` удалён — пульс задачи в
  `task_instance.last_heartbeat_at`; `dataset*` → `asset*`.
- SLA (`sla=`) удалён; замена в 3.1+ — Deadline Alerts (не подключены).
- `airflow.settings.engine`/`Session` в процессе задачи сняты (`block_orm_access`,
  `sdk/execution_time/supervisor.py:327`) — как и `create_session`.
- У `RuntimeTaskInstance` нет `queued_dttm`, у `DagRun` в контексте — `get_task_instance`:
  соседей по рану — `ti.get_task_states(...)`, остальное — SQL.
- Сообщение celery — `execute_workload` с JSON `ExecuteTask` (в нём токен задачи), а не
  `airflow tasks run`. Подключение из задачи — `airflow.sdk.Connection.get`.
- Логи задач в S3 без gzip (`*.log`, не `*.log.gz`).
- `current_schema()` не годится для поиска схемы метабазы: на стенде AF3 есть пустая `main`
  рядом с таблицами в `public`; схему даёт `af_meta.schema()` (куда разрешается `dag_run`).

## `plugins/af_meta.py` — метабаза без ORM

Решение 21.09.2026: SQL по подключению метабазы (`conns.pg` из `ctl_config`, по умолчанию
`airflowdb`) и Task SDK, без REST и служебной учётки. `add_note`, пулы, пауза сохранили
прежний интерфейс — вызовы в дагах не менялись. Цена: запись в таблицы Airflow мимо его API,
SQL привязан к схеме 3.3.1 и сверяется при обновлении Airflow.

**Запуск DAG-а** (`trigger_dag_run`) — сообщение `TriggerDagRun` супервизору, тот же канал,
что у `TriggerDagRunOperator`, но изнутри задачи: `ctl_add_chk` сенсора одной задачей
проверяет загрузку, запускает DAG и переводит её в CTL в `WAIT-AF`. Интерфейс внутренний —
его держит `tests/test_af_meta_trigger.py`.

## Что есть

| Путь | Что |
|---|---|
| `plugins/` | `utils`, `ctl_utils`, `ctl_core`, `s3_utils` (порт `af_dags@d9813ec`) + `af_meta` |
| `ctl_worker/` | оркестрация CTL (порт `af_dags@d9813ec`), без `ctl_tfs.py` — тракт ТФС отдельно |
| `check/` | служебные даги (порт `af_dags@8822591`): чистка метабазы, логов и очередей, сторож метабазы, отчёт журнала, здоровье, проверка подключений, Kafka; `test_dags` ещё не перенесён |
| `tools/` | S3-инструменты, `dummy`, `test_package` (порт `af_dags@8822591`) |
| `check/test_hrp_operators.py` | регрессионный стенд `hrp_operators` (порт `af_dags` v1.1), см. docstring |
| `tests/` | сторож внутреннего интерфейса запуска DAG-а (`python -m pytest tests`, окружение AF3) |

`plugins/` — обычный пакет (`__init__.py`): namespace-пакет проигрывает любому одноимённому
обычному пакету на `sys.path`.

Проверка — только на стенде AF3 (`etl-core-3/docs/STAND.md`, раздел «Живой AF3»): даги
кладутся в `/opt/aftest/home3/dags/`. Сквозной прогон CTL — эмулятор `testbed/ctl_worker` из
`af_dags`; переменные `ctl_*` и подключения GP стенда — копией из метабазы AF2.
