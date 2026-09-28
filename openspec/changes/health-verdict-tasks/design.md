## Context

Мотивация — `proposal.md`, «Why». Что сейчас: `report_health(checks, context, ttl_sec)` в
`plugins/utils.py` пишет отчёт `system_health/checks/<dag_id>.json`; его зовут итоговые
таски пяти дагов (коммит c072ffa), каждый — перед своим `raise`. Параметры форм сохраняет
`store_params_task`, но три дага держат свою копию его тела, а `system_health` зовёт
`store_params` изнутри `check`. Core (`mcp_health.PLUGIN_TAG = "health"`) ждёт отчёт от
каждого активного дага с тегом `health`.

## Goals / Non-Goals

**Goals:** один механизм итога для всех плагинов; единственное место записи отчёта;
одинаковые имена этапов и обвязка у дагов `tools`.

**Non-Goals:** таски на один объект (`test_connections` по подключению, перепроверки
`test_dags` по DAG'у) не переделываются — падают, как и раньше, чтобы клетка называла объект;
вердикт по ним считает сводка `report`, прямой upstream `health_*`; чистильщики (`clean`) не
становятся плагинами; `priority_weight`/таймауты долгих дагов не трогаются.

## Decisions

**Вердикты едут через XCom ключом `health`, а не через состояние таска.** Проверочный таск
зовёт `push_health(checks, context)` — `{имя: {status, summary, ...}}`, тот же формат, что
принимает `report_health`. Альтернатива — читать заметки или `return_value`: заметки режутся
до 1000 символов, `return_value` у дагов разной формы.

**Фабрика `health_tasks(ttl_sec, skill)` в `plugins/utils.py`** возвращает два таска с
`trigger_rule=ALL_DONE`, оба — прямые потомки проверочных. Каждый собирает `health` со всех
upstream (`context['task'].upstream_task_ids`; у mapped — список по индексам) и состояния
этих upstream одним запросом к `task_instance`. Upstream `failed`/`upstream_failed` без
своего вердикта превращается в проверку `<task_id>` = `error` «таск не выполнился» с первой
строкой его заметки. Альтернатива — один таск `health` с тремя исходами: отвергнута
пользователем, два таска видны в сетке отдельными строками.

**Отчёт пишет только `health_errors`** — он выполняется всегда (`ALL_DONE`) и видит то же,
что `health_warn`. Писать из обоих — гонка за один ключ S3.

**Падение `health_errors` делает ран красным по правилу Airflow**: ран `failed`, если упал
лист. Отдельный «зелёный лист» не вводится — красный ран при ошибке и нужен.

**`test_connections`: важность по шаблонам `conn_id`** (`fnmatch`, параметр `critical`,
сохраняется штатным `params`). Умолчание: `airflowdb`, `ctl`, `s3`,
`conf.get('logging', 'remote_log_conn_id')`. Таск ↔ подключение — словарь `task_id →
conn_id`, собираемый при построении групп: `_safe_id` не обратим. Вердикт считает `report`
(бывший `summary`): `connections_critical` — `error` по упавшим важным, `connections` —
`warn` по упавшим вспомогательным. Таски подключений падают как прежде; чтобы
`health_errors` не превращал упавшее вспомогательное подключение в ошибку по правилу
«упавший upstream», фабрика смотрит только на прямых upstream, а прямой upstream здесь —
`report`, который не падает.

**`system_health`: `check` → `params` + `collect` + `report`.** `collect` снимает проверки
и возвращает `result` (XCom штатно, ручной `xcom_push` уходит), `report` пишет заметку и
`push_health`. `_prev_import_error_id` на переходный период читает XCom прошлого рана из
`collect` или `check`.

**`queue_analyze` — плагин с потолком `warn`.** Вердикт строится из готовых `conclusions()`
в `report`; `error` не выдаётся. Расписание по умолчанию `'10 6 * * *'` (start_date в UTC =
09:10 MSK).

**Навыки — файлами `tools/skill/tools-*.md`.** `tools_mcp_skills` публикует каждый
`*/skill/*.md` под именем файла, код не меняется. Поле `skill` в отчёте — имя навыка дага.

## Risks / Trade-offs

- [История сетки по старым task_id теряется] → разово; в описании PR.
- [Первый прогон `system_health` после выкладки не видит базу для «новых ошибок
  импорта»] → чтение из `collect` или `check`.
- [На контуре `schedule` в `tools_queue_analyze_params` сохранён пустым — суточный прогон не
  включится] → сказать в PR; включить формой с `save_params`.
- [`is_paused_upon_creation=False` действует только при первом появлении дага] → на
  контурах, где плагин уже на паузе, включить руками; иначе core пишет «нет отчёта (пауза)».
- [Навык `airflow-health` в etl-core упоминает задачу `check`] → правка текста в etl-core #85.

## Migration Plan

Выкладка дагов одним заходом с `plugins/utils.py` (фабрика нужна всем шести). Откат —
возврат файлов; отчёты плагинов в бакете перезапишутся следующими прогонами.
