# Ревью: стандартные операторы в регрессе (`test_standart`)

Дифф `ac5ff0d..ba3ba26`, `tools/` (≈41 КБ) + спека. Ревьюеры: DeepSeek `deepseek-v4-pro` (effort high),
agy `gemini-3.8-flash-high` (Pro в `agy models` нет). Находки сверены по коду и стенду AF 3.3.1.

## Принято и исправлено

| Находка | Кто | Исправление |
|---|---|---|
| `report`: подгруппа `skip_graph` выводится в основном блоке, а не в «Стандартные операторы» (REQ-hrp-regress-44) | оба | `std` дополнен группой `skip_graph` |
| `tools/readme.md` говорит «в каждой группе — `*.tpl`», у сенсоров и `TriggerDagRun` шаблон в основном тесте | agy | в абзаце о стандартных операторах — где шаблон у них |

## Принято как Minor, без правки

- `trigger_dagrun_conf`: «нет ранов» = «нет дага» — упрощение помечено в коде (`tools_dummy` ежечасный);
  на свежем стенде без ранов тест уйдёт в ☮️ с причиной, не в ❌.
- `trigger_dagrun_conf`: `conf` созданного рана сверяется по исключению оператора, а не по метабазе — ран
  создаёт `_handle_trigger_dag_run` из того же исключения, проверка `run_id` в метабазе есть.
- `short_circuit_both`: у оператора в тесте нет downstream, исход «нет» ничего не пропускает
  (провайдер: «No downstream tasks; nothing to do»); настоящий пропуск проверяет `skip_graph` (REQ-hrp-regress-45).
- `_ctx_fn` сверяет `ti`/`run_id`, не `ds`; `SQLExecuteQueryOperator` не возвращает `row_count` — сверять нечего.
- Открытый Minor плана: при `op_skip_graph=false` задачи `branch`/`short`/`keep` всё равно исполняются, побочных эффектов нет.

## Отклонено (проверено)

- «Нет шаблонного теста у `ch_sensor`» (DeepSeek Critical) — `ch_sensor_ok` передаёт `sql` шаблоном `{{ 0 }}`.
- «`task_policy` до `prepare_for_execution` не действует на исполняемый оператор» (DeepSeek Critical) —
  `prepare_for_execution` копирует оператор уже после политики; в бою политика тоже при разборе.
- «`S3CreateBucketOperator` всегда зовёт `create_bucket`, тест гарантированно падает» (agy Critical) —
  провайдер на стенде: `if not self.hook.check_for_bucket(...)`, ран 13:33:42 — ✅.
- «Подмена `skip_all_except` может не перехватить ветвление» (DeepSeek) — на стенде оба исхода ✅.
- «Нижняя граница тайм-аута сенсора флакнет» (agy) — сенсор падает по истечении `timeout`, не раньше.
- Гонки моков между задачами, задержка S3, неубранные таблицы (DeepSeek) — задачи в отдельных процессах,
  уборка в `t.cleanup()`.

## Отложено (не эта ветка)

etl-core develop_3: подключение `af_api` в ядре на внутренний адрес API (+ `test_connections`, сверка с пода
воркера альфы на SSO) и фикс патча ClickHouse (`ch_operator`, `ch_branch_sql`, `ch_sql_execute` ❌ до него).
