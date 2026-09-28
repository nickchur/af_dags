---
name: tools
description: Индекс служебных дагов Airflow (каталог tools/, на сигме CI06932748/tools) — какой даг отвечает на какой вопрос, общее устройство (пул, сохраняемые параметры, расписание), итог дагов-проверок в тасках health_warn / health_errors, теги-роли. Подробности по дагам — навыки tools-system-health, tools-pg-activity, tools-log-events, tools-test-connections, tools-test-dags, tools-queue-analyze, tools-paused-runs. Используй, когда спрашивают про любой tools_*, «tools_* красный», «health_errors / health_warn», «plugins: нет отчёта», «как поменять расписание служебного дага», «где сохраняются параметры», «нет навыка на MCP / нет текста в DAG Docs», «метабаза растёт».
---

# Служебные даги (`tools/`) — индекс

*2026-09-28 12:38 MSK · v2.4 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Навык для агента GigaCode с MCP-сервером Airflow (сигма и альфа). Источник правды — каталог
`tools/` репозитория `af_dags`: `tools/readme.md` и шапка каждого модуля; при расхождении
прав код. На сигме каталог лежит как `CI06932748/tools/…`, общие функции — модуль
`CI06932748.tools.utils` (в репозитории — `plugins/utils.py`). `s3_tools/` в навык не входит.

У тебя только чтение через MCP: раны, состояния задач, заметки. Даги ты не запускаешь, паузу
не снимаешь, галочки не ставишь — советуешь человеку, **что запустить и с какими
параметрами** (раздел 5).

## 0. Куда идти

| Вопрос | Навык |
|---|---|
| Что показал `tools_system_pulse` / `tools_system_health`, `control`, `workers: 0`, ошибки импорта, плагин молчит | **`tools-system-health`** |
| `dag_size`, даг больше 300 тасков | **`tools-test-dags`** |
| Сессии и блокировки метабазы, `tools_pg_activity` | **`tools-pg-activity`** |
| Задачи висят в `queued`, сбои доставки, `tools_log_events` | **`tools-log-events`** |
| Подключение не работает, `tools_test_connections` | **`tools-test-connections`** |
| Сериализация дрожит, дубль `dag_id`, время разбора, `tools_test_dags` | **`tools-test-dags`** |
| Задачи висят в `scheduled`, очередь стоит, мусор в брокере | **`tools-queue-analyze`** (сначала `get_system_health` по навыку **`airflow-health`**) |
| Раны запаузенных дагов, `runs.paused_active` | **`tools-paused-runs`** |
| Загрузки CTL / ЕР / ТФС | **`ctl-worker`** / **`er-export`** / **`tfs-kafka`** |

## 1. Общее устройство

- **Этапы и имена тасков одинаковы у всех дагов:** `params` → `collect` → действие (`clean`,
  `close`, `purge`, `terminate`, `sweep`, `publish`, `save`…) → `report` → у плагинов здоровья
  `health_warn` / `health_errors`. 28.09.2026 переименованы:
  `system_health.check` → `collect` + `report`, `paused_runs_cleanup.find` и
  `show_connections.show_connections` → `collect`, `summary` → `report` (`test_connections`,
  `test_dags`, `test_hrp_operators`); в старых ранах — старые имена.
- **Итог дага-проверки — в двух последних тасках.** Сбор и сводка находками не падают:
  - ⚠️ `health_warn` зелёный с заметкой — есть предупреждения; ☮️ — нет;
  - ❌ `health_errors` красный, ран красный, уведомление — есть ошибки; ☮️ — нет;
  - красный **сбор** (`collect`, `report`) — сломалась сама проверка, `health_errors` назовёт
    его «не выполнился».
  Исключение — таск на один объект (подключение, перепроверка одного DAG'а): он красный, чтобы
  клетка называла объект, а ран краснеет только по вердикту сводки.
- **Теги — роль:** `health` — плагин здоровья, `health_errors` пишет отчёт в
  `system_health/checks/<dag_id>.json`, его читает `get_system_health` → `plugins`; `clean` —
  удаляет; `AutoQA` — регрессия и проверка «работает ли» (`dummy` раз в час). Плагины: `system_pulse`, `system_health`, `pg_activity`, `log_events`,
  `test_connections`, `test_dags`, `queue_analyze`.
- **Пул `tools_pool`** (16 слотов). Короткие проверки идут с `priority_weight 900`,
  `weight_rule='absolute'`: выше регрессии, ниже агента CTL (999/1000). Тяжёлые `test_dags`,
  `test_hrp_operators` и чистильщики приоритета не получают намеренно.
- **Сохраняемые параметры.** Форма запуска предзаполняется из Variable `tools_<имя>_params`
  (у `pg_activity` — `tools_pg_activity_cfg`), при её отсутствии — из кода. Записывает её
  только запуск с галочкой **`save_params`** — таск `params` (☮️, если галочки нет или значения
  не изменились). Что сохранено, смотри MCP `get_variable_value("tools_<имя>_params")`:
  значение — форма и `schedule`, `description` — когда и каким раном записано. Без аргумента
  инструмент перечисляет ключи; `local_connections` через MCP закрыта намеренно.
- **Расписание — тоже параметр** `schedule` (cron или пресет; пусто или `None` — только
  вручную). Новое применяется **со следующего разбора файла**. Негодное значение таск `params`
  не записывает и падает ❌; уже записанное битым игнорируется в пользу кода.
- **Разовые галочки не сохраняются никогда**: `purge` (`queue_analyze`), `terminate`
  (`pg_activity`), `purge_docs` (`mcp_skills`), `cleanup_deleted` (`test_dags`). Исключение —
  `close` у `paused_runs_cleanup`: сохраняемый, чтобы закрывали и плановые запуски.
- **Создаются на паузе**: `db_cleanup`, `log_cleanup`, `paused_runs_cleanup`, ручные
  `test_kafka_*`. Плагины здоровья, `dummy`, `mcp_skills`, `show_connections`,
  `test_hrp_operators` включаются сами. Плагин на паузе — в `get_system_health` «нет отчёта»;
  на паузе он обычно по решению человека.
- **Итог — в заметках** рана и задач (`add_note`): ✅ / ❌ / ☮️. XCom тебе недоступен.

## 2. Даги

| Даг | Расписание | Что делает | Что меняет |
|---|---|---|---|
| `tools_system_pulse` | `2-59/5 * * * *` | Лежит ли контур: компоненты, celery, control, метабаза, доставка | ничего |
| `tools_system_health` | `7 * * * *` | Почему задачи не идут: S3 логов, пулы, разбор, раны, `scheduled`; сторож отчётов плагинов | ничего |
| `tools_pg_activity` | `*/10 * * * *` | Сессии и блокировки метабазы | сессии — только при `terminate` и `dry_run=False` |
| `tools_log_events` | `30 6 * * *` | Сбои доставки по журналу `log` | ничего |
| `tools_test_connections` | `15 23 * * *` | Доступность каждого подключения, важные — `critical` | ничего |
| `tools_test_dags` | `0 23 * * *` | Дрожание сериализации, версии в S3, время разбора | ничего |
| `tools_queue_analyze` | `10 6 * * *` (09:10 MSK) | Почему задачи ждут; брокер | брокер — только при `purge` |
| `tools_paused_runs_cleanup` | `0 * * * *` | Раны у запаузенных дагов | Mark failed — только при `close` |
| `tools_db_cleanup` | `0 2 * * *` | Чистка метабазы старше `retention_days` (180) | **удаляет**; `dry_run=False` по умолчанию |
| `tools_log_cleanup` | `17 5 * * *` | Сроки хранения по папкам бакета логов | **удаляет** обходом; при `lifecycle` ещё и правило жизненного цикла |
| `tools_show_connections` | `0 23 * * *` | Подключения secret backend → Variable `local_connections` | Variable |
| `tools_mcp_skills` | `*/30 * * * *` | Навыки `*/skill/*.md` → `mcp_skill__*`; оглавление документации | Variables |
| `test_hrp_operators` (без префикса) | `@once` | Регрессия операторов `hrp_operators` | тестовые таблицы и файлы, убирает за собой |
| `tools_test_kafka_snd` / `_rcv` | вручную | Разовая отправка / просмотр топика | отправка **мимо очереди** тракта ТФС |
| `tools_dummy` | `3 * * * *` | Шедулер и воркер живы: `dummy_task` (`EmptyOperator`, отмечает шедулер) → `ping` на воркере; в заметке `ping` — сколько думал шедулер и ждала очередь; красный по `dagrun_timeout` (50 мин) | ничего |

## 3. Разбор по симптому (общее)

| Симптом | Куда смотреть |
|---|---|
| `get_system_health` → `plugins`: «плагин tools_X: нет отчёта» | даг на паузе, не запускался после выкладки или расписание снято (`schedule` пуст в Variable `*_params`) |
| «последний отчёт N назад» | прогоны не идут: раны дага, пул `tools_pool`, очередь |
| статус проверки в отчёте плагина | итог дага; поле `skill` проверки называет навык с толкованием |
| `tools_*` ❌ на таске `params`, есть `start_date`, в логе «не cron и не пресет» | негодное `schedule` в форме **ручного** запуска с `save_params`; переменная не тронута, остальные таски — `upstream_failed` |
| `tools_*` ❌ на таске `params`, `start_date` пуст, в логе `http://:8080/…: No host supplied` | таск **не стартовал ни на одном воркере**: его сняли в очереди. Расписание ни при чём. Смотри `tools_log_events` за это время и `platform.scheduled`/`queued` в Health; совет — перезапуск рана, Variable не трогать |
| На MCP нет навыка / в DAG Docs нет текста | `tools_mcp_skills` (каждые 30 мин): новый навык появляется до получаса спустя после выкладки дагов; навык — Variable `mcp_skill__<имя>`; текст документа хранится, только если `store_docs` (сигма) |
| `tools_dummy` ❌, `dummy_task` не зелёный | стоит шедулер: `get_system_health` → компоненты |
| `tools_dummy` ❌, `ping` не стартовал | задачи не доходят до воркера: `tools_system_pulse` (`celery`, `control`, `delivery`) и `tools_log_events` за этот час |
| `tools_dummy` зелёный, в заметке `ping` шедулер или очередь — минуты | задержка планирования или доставки: `tools_queue_analyze`, `tools_log_events` |
| Метабаза растёт | `tools_db_cleanup`: последний ран, заметка с размерами схемы и дельтой |

## 4. Норма, а не тревога

- ☮️ `params` — запуск без `save_params` или без изменений.
- ☮️ `health_warn` и `health_errors` — всё здорово.
- ☮️ `purge` / `close` / `terminate` — галочка не стояла.
- ❌ таск подключения при зелёном ране `tools_test_connections` — упало вспомогательное.
- Даг `tools_*` из старых ранов: `tools_queue_cleanup` с 24.09.2026 — `tools_queue_analyze`;
  `dummy_dag` — `tools_dummy`.

## 5. Чего не советовать и что — человеку

- `purge`, `close`, `terminate`, `purge_docs`, `db_cleanup` с `dry_run=False` — запускает
  **человек**. Ты говоришь, с какими параметрами, и что увидит в заметке.
- Сменить расписание или список важных подключений: запуск с новыми параметрами и
  `save_params` — без выкладки.
- `test_kafka` не для регулярной отправки: мимо очереди и лимитов тракта ТФС.

## 6. Как отвечать

Коротко: **что видно** (даг, ран, заметка — цитатой ключевой строки), **что это значит** по
навыку дага, **что сделать** и кто это делает. Если нужного рана нет или он старый — так и
скажи и предложи запуск с конкретными параметрами.
