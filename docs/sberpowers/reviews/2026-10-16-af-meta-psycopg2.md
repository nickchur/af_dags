# Ревью: движок `af_meta` на psycopg2 (REQ-plugins-30)

- Дата: 2026-10-16
- Ревьюеры: DeepSeek v4-pro, agy gemini-3.8-flash-high (только чтение); свод — исполнитель
- Проверка: стенд AF3 — `tools_system_health` success (runs, pools, parsing, scheduled), в `tools_system_pulse`
  проверки метабазы (components, metabase, delivery) зелёные; красный `control` — на стенде нет брокера redis,
  так было и до правки. Связка с ядром: со старым перехватом `postgresql+psycopg2` подключается, `+psycopg` — нет.

## Принято

- Соответствие REQ-plugins-30 (DeepSeek): подтверждено, работает и с ядром 3.0.0, и с 3.0.1.

## Отклонено

- Потеря extra подключения (agy, высокая): `sqlalchemy_url` провайдера 7.0.1 extra и раньше не нёс (берёт только
  `sqlalchemy_query`) — поведение движка то же, что до правки; у `airflowdb` на альфе extra не обязателен (sslmode
  prefer/disable). DeepSeek утверждал обратное (extra в query) — по коду провайдера неверно; формулировка спеки верна.
- Жёсткая зависимость от psycopg2 (DeepSeek, низкая): psycopg2 в образе обязателен — на нём метабаза и failover ядра.
- Тест на `_engine` (DeepSeek, средняя): проверено прогоном дагов на стенде; юнит-тест без Airflow-хука был бы моком
  самого себя.
