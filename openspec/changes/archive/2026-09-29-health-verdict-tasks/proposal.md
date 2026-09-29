# Даги-проверки завершаются вердиктом здоровья, а не падением

## Why

Даг-проверка из `tools/` сейчас падает сам, когда находит неисправность: `system_health` —
на `error`, `log_events` — выше порога, `pg_activity` — при находках с `alert`,
`test_connections` и `test_dags` — в сводке. Красный ран поэтому значит одно из двух:
«проверка сломалась» или «проверка нашла проблему», и по сетке их не различить. Отчёт плагина
здоровья (тег `health`, контракт с `get_system_health`) пишется из разных мест пяти дагов, а
`system_health` вручную кладёт XCom перед падением, чтобы лента не теряла красные прогоны.

28.09.2026 на dev это мешало разбору: упавший по таймауту метабазы `system_health` выглядел
так же, как `system_health`, нашедший неисправность.

## What Changes

- Проверочные таски находками больше не падают. Вердикты уходят в XCom, итог — в двух
  последних тасках дага: `health_warn` (skip без предупреждений, ✅ с заметкой ⚠️ при них) и
  `health_errors` (skip без ошибок, ❌ при них — ран красный, уведомление штатным колбэком).
  Отчёт плагина пишет только `health_errors`. Упавший проверочный таск — тоже ошибка.
- Общая фабрика этих тасков — в `plugins/utils.py`; ею пользуются шесть плагинов:
  `system_health`, `pg_activity`, `log_events`, `test_connections`, `test_dags`,
  `queue_analyze`.
- `test_connections`: подключения делятся на жизненно важные (параметр `critical`, шаблоны
  `conn_id`, сохраняется в Variable) и вспомогательные. Упавшее подключение по-прежнему
  роняет свой таск, но ран краснеет только из-за важного; вспомогательное — предупреждение.
- `queue_analyze` запускается раз в сутки (09:10 MSK по умолчанию) и становится плагином;
  его вердикт не выше предупреждения, удаление из брокера остаётся разовой галочкой.
- **BREAKING (имена тасков).** Единые имена этапов: `params` → `collect` → действие →
  `report` → `health_warn`/`health_errors`. `system_health.check` → `collect` + `report`,
  `paused_runs_cleanup.find`, `show_connections.show_connections` → `collect`
  (`log_cleanup.layout` — действие, остаётся); `summary` → `report` у `test_connections`, `test_dags`, `test_hrp_operators`.
  История старых task_id в сетке не переносится.
- Единообразие: таск `params` через общий `store_params_task` у всех дагов с расписанием
  (включая `system_health`); плагины создаются включёнными; теги — роли (`health`, `clean`,
  `check`, `AutoQA`), предметные теги снимаются.
- Навык `tools` делится на индекс и навыки дагов (`tools-system-health`, `tools-pg-activity`,
  `tools-log-events`, `tools-test-connections`, `tools-test-dags`, `tools-queue-analyze`,
  `tools-paused-runs`); поле `skill` в отчёте плагина указывает навык своего дага.

## Capabilities

### New Capabilities

Нет.

### Modified Capabilities

- `tools`: новые требования «Даги-проверки завершаются вердиктом здоровья» и «Разбор очереди
  раз в сутки»; меняются «Каждое подключение проверяется своей задачей» (важные и
  вспомогательные), «Сводка по трём состояниям» (таск `report`), «Надзор за сессиями
  метабазы» (`alert` задаёт уровень), «Проверка сериализации никогда не роняет прогон»,
  «Сбои доставки задач видны числом», «Состояние контура снимается раз в час» (падает
  `health_errors`, а не проверка).

## Impact

- Код: `plugins/utils.py`; `tools/system_health.py`, `pg_activity.py`, `log_events.py`,
  `test_connections.py`, `test_dags.py`, `queue_analyze.py`, `paused_runs_cleanup.py`,
  `show_connections.py`, `log_cleanup.py`, `db_cleanup.py`, `test_hrp_operators.py`,
  `mcp_skills.py`, `dummy.py` (теги); `tools/readme.md`, `tools/skill/*.md`.
- etl-core: навык `airflow-health` ссылается на «лог задачи `check`» `tools_system_health` —
  текст правится в открытом etl-core #85.
- Контуры: на сетке старые task_id показываются как removed; при сохранённом пустом
  `schedule` в `tools_queue_analyze_params` суточный прогон не включится сам.
