# Delta-спека: движок метабазы `af_meta` на psycopg2

- Дата: 2026-10-16
- Статус: согласована пользователем 16.10.2026 («да, делай оба»)
- Baseline: docs/sberpowers/specs/2026-10-15-s3-pg-stream-operator.md (нумерация REQ-plugins)
- Источник: разбор здоровья альфы dev 16.10.2026 (лог tools_system_pulse.collect)

## Мотивация

На альфе AF3 все сессии `af_meta.meta_session()` падали «invalid dsn: invalid connection option "context"»:
провайдер postgres на SQLAlchemy 2 строит движок `postgresql+psycopg`, а перехват подключения в ядре
(etl-core до 3.0.1) отдаёт его параметры в psycopg2. Ядро чинится в 3.0.1 (REQ-af3-62 etl-core);
даги не должны ждать выкладки ядра.

## ADDED

- **REQ-plugins-30** — **сессия метабазы работает при любом ядре.** `meta_session()` подключается драйвером
  psycopg2 независимо от того, какой драйвер выбирает провайдер postgres; хост, порт, учётка, БД и
  `search_path` — как раньше.

## Non-goals

- `af_meta.sql()`: идёт через `hook.get_conn()` без SQLAlchemy, не затронут.
- Extra подключения в движке: как и раньше, `sqlalchemy_url` их не несёт.

## Критерии приёмки

- Дано: альфа dev, ядро 3.0.0 → ран `tools_system_pulse` → проверки celery/control/metabase/delivery
  без «invalid connection option "context"».
- Дано: стенд AF3 → ран `tools_system_health` → runs/pools/parsing/scheduled зелёные, как до правки.
