# Подводные камни

## Длинная подпись mapped-таска роняет сохранение экземпляра

- **Контекст:** `map_index_template` с данными из внешнего источника (адреса, имена, списки).
- **Корень:** колонка `task_instance.rendered_map_index` — `String(250)`, Airflow 2.11.2 пишет
  результат шаблона без обрезки (`_render_map_index`); на Postgres длиннее — ошибка записи, таск
  падает уже после работы.
- **Как избежать/чинить:** обрезать подпись до 250 символов в коде (в `tools_test_connections` —
  `LABEL_MAX`, обрезка с «…»); при переходе на Airflow 3 сверить колонку заново.
- **Источник:** docs/sberpowers/worklog/2026-10-07-test-connections-host-port.md

## Текст исключения stdlib выносит в лог фрагмент значения

- **Контекст:** разбор адресов и чисел из extra/подключений с предупреждением в лог.
- **Корень:** сообщения `ValueError` содержат разбираемую строку: `urlsplit(...).port` —
  `Port could not be cast to integer value as '<порт>'`, `int()` — `invalid literal … '<строка>'`.
- **Как избежать/чинить:** в лог — постоянный текст причины, исключение не форматировать.
- **Источник:** docs/sberpowers/worklog/2026-10-07-test-connections-host-port.md

## `extra_dejson` бывает не словарём

- **Контекст:** код, который делает `conn.extra_dejson.get(...)`.
- **Корень:** при валидном JSON не-объекта (`[1]`, `"x"`) Airflow 2.11 возвращает список или
  строку как есть; `{}` — только при пустом или битом extra.
- **Как избежать/чинить:** проверять `isinstance(extra, dict)` перед `.get`; в
  `tools_test_connections` это сделано в `_conn_addr`, в `_run_test` (Kafka, Redis) — ещё нет.
- **Источник:** docs/sberpowers/worklog/2026-10-07-test-connections-host-port.md
