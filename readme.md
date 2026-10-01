# af3_dags — даги HR-платформы под Airflow 3

*2026-10-01 23:40 MSK · v0.5 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

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
- `[core] dags_folder` не указывает на даги, пришедшие бандлом: корень — от `__file__`.
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
| `plugins/` | `utils`, `s3_utils` (порт `af_dags@364f822`) + `af_meta` |
| `ctl_worker/` | оркестрация CTL, с `ctl_core` и `ctl_utils` (порт `af_dags@364f822`), без `ctl_tfs.py` — тракт ТФС отдельно |
| `tools/` | служебные даги (порт `af_dags@364f822`, бывший `check/`): чистка метабазы, логов, раны запаузенных дагов, сторож метабазы, разбор очереди, отчёт журнала, здоровье, проверка подключений, Kafka, навыки MCP, регрессия `hrp_operators` (+ группы `click_to_click`, `lineage`); `test_dags` не перенесён — см. ниже |
| `s3_tools/` | S3-инструменты, `test_package` (порт `af_dags@364f822`, бывший `tools/`) |
| `tests/` | сторож внутреннего интерфейса запуска DAG-а (`python -m pytest tests`, окружение AF3) |

`plugins/` — обычный пакет (`__init__.py`): namespace-пакет проигрывает любому одноимённому
обычному пакету на `sys.path`.

Проверка — только на стенде AF3 (`etl-core-3/docs/STAND.md`, раздел «Живой AF3»): даги
кладутся в `/opt/aftest/home3/dags/`. Сквозной прогон CTL — эмулятор `testbed/ctl_worker` из
`af_dags`; переменные `ctl_*` стенда — копией из метабазы AF2.

**`tools_test_dags` не перенесён.** Он ловит нестабильную сериализацию по смене `dag_hash`;
в AF3 такая смена заводит новую `dag_version`, и проверке нужно своё решение, а не замена SQL.

**У стенда AF3 свой эмулятор CTL и свои бакеты** (с 01.10.2026; до того общие с AF2, и сенсор,
монитор и симулятор AF3 двигали загрузки AF2, а `tools_mcp_skills` переписывал его `docs/`):

| Что | AF2 | AF3 |
|---|---|---|
| Эмулятор CTL | `ctl-mock`, порт 9080 | `ctl-mock3`, порт 9081 (`/opt/aftest/ctl-mock3`) |
| База GP и состояние эмулятора | `gp_test` | `gp_test3` (копия `gp_test`) |
| Подключение `ctl` | `/vault/secrets/application` | `/vault/secrets/application3` (`stand_af3_env.sh`) |
| `alpha-adb_dev_comm-read` / `-write` | → `gp_test` | → `gp_test3` (метабаза AF3) |
| Бакеты `ctl_config` | `edpetl-ctl`, `edpetl-files` | `edpetl-ctl3`, `edpetl-files3` |
| Бакет логов | `hrplt-test` | `hrplt-test3` (`stand_af3_env.sh`) |

Общими остались MinIO, ClickHouse и Kafka стенда. Прогон: загрузку создаёт
`POST :9081/v4/api/wf/<wf_id>/loading`, затем START в RUNNING переводит
`PUT :9081/v4/api/loading/<lid>/status` с `{"status": "RUNNING", "log": ""}` — в бою это делает
движок CTL, эмулятор сам статус не двигает. Лог обязательно пустой: RUNNING с логом сенсор
пропускает как «already run». Монитор START тоже перезапускает, но только через `new_grace`
(час).
