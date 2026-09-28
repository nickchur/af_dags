---
name: tools-system-health
description: tools_system_pulse (раз в 5 мин — компоненты, celery, control-канал celery и брокер, метабаза, доставка) и tools_system_health (раз в час — бакет логов, пулы, разбор файлов, раны, scheduled, сторож отчётов плагинов) и их отчёты в get_system_health → plugins. Используй, когда спрашивают «что показал tools_system_health / tools_system_pulse», «plugins.tools_system_health», «plugins.tools_system_pulse», «control warn/error», «workers: 0 при идущих задачах», «воркеры перезапускаются по liveness», «ошибки импорта», «плагин молчит», «tools_system_health красный».
---

# `tools_system_pulse` и `tools_system_health` — состояние контура

*2026-09-28 12:38 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Общее про служебные даги и концепцию `health_warn` / `health_errors` — навык **`tools`**.

Два DAG'а, код и пороги общие (`tools/system_health.py`), до 28.09.2026 был один часовой:
- **`tools_system_pulse`**, раз в 5 минут — `components`, `celery`, `control`, `metabase`,
  `delivery`. Таски `collect` (с заметкой) → `health_warn` / `health_errors`. Срок отчёта 15 мин;
- **`tools_system_health`**, раз в час — `s3_logs`, `pools`, `parsing`, `runs`, `scheduled`,
  `plugins`. Таски `params` → `collect` → `report` (заметка) и `health_warn` / `health_errors`.
  Срок отчёта 2 ч 10 мин.

`dag_size` теперь в `tools_test_dags` (навык **`tools-test-dags`**), таблиц метабазы здесь больше
нет — их размеры в заметке `tools_db_cleanup`. `collect` находками **не падает**:
красный `collect` значит, что сломалась сама проверка. Итог:
- ⚠️ `health_warn` зелёный с заметкой — есть предупреждения;
- ❌ `health_errors` красный, ран красный, уведомление — есть ошибки; в заметке строка на ошибку;
- оба ☮️ — всё здорово.

Отчёт плагина (`get_system_health` → `plugins.<dag_id>.checks.<имя>`) пишет `health_errors`. Полный результат — XCom `return_value` таска `collect`
(`{status, reasons, checks, took_sec}`); тебе он недоступен, читай заметку `report` и отчёт.

## Проверки и что советовать

| Проверка (DAG) | Симптом | Что значит и что советовать |
|---|---|---|
| `components` (пульс) | ❌ | метабаза, шедулер или dag-processor `unhealthy` — навык `airflow-health` |
| `celery` (пульс) | ⚠️ ждут при занятых воркерах | не хватает слотов: ёмкость, не авария |
| `control` (пульс) | ⚠️/❌, `workers: 0` в карточке Health при идущих задачах, воркеры перезапускаются по liveness («celery молчит 1201 с») | см. ниже |
| `s3_logs` (час) | ❌ | бакет логов не принимает запись — задачи повиснут на записи лога (11.09.2026) |
| `delivery` (пульс) | ⚠️ > 60 с | задача долго ждала воркера — очередь или слоты |
| `parsing` (час) | ⚠️ | новые ошибки импорта (по id строки `import_error`), отставший файл, стоит разбор целиком |
| `runs` (час) | ⚠️ | раны у запаузенных дагов — навык **`tools-paused-runs`** |
| `scheduled` (час) | ⚠️ | задачи застряли не на лимите своего дага — навык **`tools-queue-analyze`** |
| `plugins` (час) | ⚠️ «молчат: tools_X (на паузе / нет отчёта / отчёт N ч назад)» | плагин перестал отчитываться: включить даг, посмотреть его последние раны и ошибки импорта. «сейчас tools_X warn/error» — справка: разбирать по навыку того дага (поле `skill` его отчёта), здесь не повторяется |

**`control` — control-канал celery и брокер.** Задачи идут через списки Redis, а ping/inspect —
через pub/sub, и второе может молчать при живом первом. Поля отчёта:
- `answered` — сколько узлов ответило на ping, `busy_pods` — на скольких подах задачи идут прямо
  сейчас; ответов меньше, чем подов, — ⚠️, ни одного — ❌. Имена не сверяются: в `job.hostname`
  IP пода, а узел celery — `celery@<имя пода>`; `nodes` — кто ответил;
- `reply_sec` — задержка самого медленного ответа: больше 2 с — карточка Health и отбивки ответов
  не дожидаются и показывают меньше воркеров, хотя все живы (dev, 27–28.09.2026);
- где задержка — по звеньям: `cmd_ms` (команда брокеру; много — узел или сеть), `pubsub_ms`
  (доставка pub/sub; много при быстрых командах — рассылка брокера), оба малы при большом
  `reply_sec` — медлят сами воркеры;
- `info.uptime_in_seconds` мал — узел брокера недавно перезапускался или сменился;
- `loopback: false` или `pattern_subs: 0` — pub/sub брокера сломан; `acl_channels` без `&*` —
  каналы закрыты правами пользователя; «не прочитаны» — команда закрыта правами, это не поломка.

Это вопрос к владельцам Redis, не к дагам.

**`collect` ❌ `canceling statement due to statement timeout`** — метабаза перегружена или
таблица раздута; сама проверка ни при чём. `health_errors` назовёт `collect` «не выполнился».
