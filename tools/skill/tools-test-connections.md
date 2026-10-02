---
name: tools-test-connections
description: tools_test_connections — ночная проверка каждого подключения secret backend; важные (critical) и вспомогательные подключения; плагин здоровья. Используй, когда спрашивают «подключение не работает», «что упало в test_connections», «tools_test_connections красный», «connections_critical», «почему ран зелёный, а подключение красное».
---

# `tools_test_connections` — доступность подключений

*2026-10-01 22:37 MSK · v1.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Общее про служебные даги и `health_warn` / `health_errors` — навык **`tools`**. Список
подключений снимает первый таск `collect` этого же рана (заметка — строка на тип; Variable
`local_connections` для выпадающих списков, через MCP закрыта). До 29.09.2026 — отдельный даг
`tools_show_connections`, в старых ранах — таски по подключениям без `collect`.

**Таски:** `params`; `collect` → mapped-таск `check`, экземпляр на подключение, подписан
«группа · `conn_id`» (группы `tfs`, `postgres`, `s3`, `ctl`, `clickhouse`, `kafka`, `trino`,
`redis`, `other`) → `report` (таблица ✅/❌/☮️, ⭐ — важное) →
`health_warn` / `health_errors`. Раз в сутки в 23:15 MSK. Упал `collect` — список не снят, проверок нет, ❌ `health_errors`
(`connections_critical` называет `collect`).

**Пропуск групп.** Флаги `skip_<группа>` (`skip_kafka`, …; сохраняются с `save_params`):
подключения группы — ☮️, не ошибка и не предупреждение. ☮️ целой группы при зелёном ране — чаще всего она.

**Важные и вспомогательные.** Параметр `critical` — шаблоны `conn_id` (fnmatch), по умолчанию
`airflowdb`, `ctl`, `s3` и подключение бакета логов; сохраняется с `save_params` в
`tools_test_connections_params`. Упавшее подключение **всегда** роняет свой таск — клетка
называет его. Но:
- упало важное — ❌ `health_errors` (`connections_critical`), ран красный, уведомление;
- упало вспомогательное — ⚠️ `health_warn` (`connections`), **ран зелёный**.

Отчёт плагина — `health_errors`, срок 26 ч.

| Что видно | Что значит |
|---|---|
| ☮️ у подключения | проверка пропущена: нет провайдера, тип не поддержан, хост не резолвится |
| ❌ подключения, ран зелёный | упало вспомогательное — ⚠️ в `health_warn` |
| ран красный | упало важное — список в `health_errors` и в `connections_critical` отчёта |
| на стенде все S3/CTL красные | стенд, а не контур: там нет Kerberos и корпоративного S3 |

Что считать важным, решает человек: запуск с новым `critical` и `save_params`.
