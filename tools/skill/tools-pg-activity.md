---
name: tools-pg-activity
description: tools_pg_activity — сторож метабазы раз в 10 минут: idle in transaction, долгие запросы, блокировки, чей это таск и жив ли он; плагин здоровья. Используй, когда спрашивают «таски умирают с обрывом метабазы», «блокировки в метабазе», «что показал tools_pg_activity», «tools_pg_activity красный», «statement timeout в collect».
---

# `tools_pg_activity` — сторож метабазы

*2026-09-28 12:18 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Общее про служебные даги и `health_warn` / `health_errors` — навык **`tools`**.

**Таски:** `params`; `collect` → `save` / `terminate` → `report` → `health_warn` /
`health_errors`. Снимки при находках — в бакете логов,
`pg_activity/<дата>/<время>.json`.

**Итог.** Находки таски не роняют: при `alert` (по умолчанию) — ❌ `health_errors`, ран красный,
уведомление; без `alert` — ⚠️ `health_warn`, ран зелёный. Отчёт плагина — `health_errors`, срок
40 мин (четыре прогона). Красный `collect` — сломалось само снятие.

| Что видно | Что значит | Что советовать |
|---|---|---|
| находка `owner_alive=true` | таск идёт и бьётся — транзакция закроется вместе с ним | ничего; висит сутками — вопрос владельцам дага |
| `owner_alive=false` | таск числится в работе, но хартбит протух, или уже завершился | брошенная сессия: запуск с `terminate` и `dry_run=False` — человеку |
| `owner_alive=null` | владельца не опознали | то же, но без имени |
| `collect` ❌ `canceling statement due to statement timeout` | его запрос не уложился в 30 с | не объясняй это блокировкой или `idle in transaction`: SELECT в PostgreSQL блокировок строк не ждёт. С v1.8 запрос идёт по индексам (до неё сканировал весь `task_instance`, ift с 01.09.2026). Падает и после — метабаза перегружена или таблицы раздуты: человеку `EXPLAIN` запроса из заметки и `n_dead_tup`, `last_autoanalyze` в `pg_stat_user_tables` для `task_instance` и `job`. Сигма-ифт 28.09.2026: падал раз в ~10 мин |

`terminate` — разовая галочка, не сохраняется; сессию живого таска сторож не убивает ни при каких
настройках. Запускает человек.
