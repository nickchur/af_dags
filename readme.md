# af3_dags — даги HR-платформы под Airflow 3

*2026-09-19 01:15 MSK · v0.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Перенос дагов из `af_dags` (Airflow 2) на Airflow 3.3.1 — этап 8 плана миграции
`etl-core-3/openspec/changes/airflow-3-migration/tasks.md`. **Приватный**: сюда можно то, что
в публичный `af_dags` класть нельзя, но пароли и ключи не кладутся и здесь.

## Чем даг AF3 отличается от дага AF2

- Импорты — из `airflow.sdk` (`dag`, `task`, `Param`, `TaskGroup`, `TriggerRule`, исключения).
- **У задачи нет метабазы.** `create_session`, `provide_session`, `TaskInstance`/`DagModel`
  из задачи не работают; состояния соседей по рану — `context["ti"].get_task_states(...)`
  (Task SDK), остальное — публичный REST API.
- **Заметок из задачи нет.** Execution API note не принимает, поэтому `add_note` и
  `on_callback` из `af_dags/plugins/utils.py` в AF3 не переносятся как есть (решение — этап 8).
- **При разборе метабазы тоже нет**: `ensure_pool` и прочие записи на импорте дага не работают.

## Что есть

| Путь | Что |
|---|---|
| `check/test_hrp_operators.py` | регрессионный стенд `hrp_operators` (порт `af_dags` v1.1), см. docstring |

Проверка — только на стенде AF3 (`etl-core-3/docs/STAND.md`, раздел «Живой AF3»): даг кладётся
в `/opt/aftest/home3/dags/<каталог>/`.
