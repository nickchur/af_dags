# Ревью: `tools_s3_to_zip` — пустые даты: авто-ран вчера, ручной сегодня (2026-10-19)

Ревьюеры: DeepSeek `deepseek-v4-pro`, agy `gemini-3.8-flash-high`. Оба: логика соответствует спеке (граница MSK,
Clear, backfill), блокирующих нет.

## Принято

| Находка | Кто | Что сделано |
|---|---|---|
| `dr.run_type == 'manual'` хрупко к enum/строке | оба | `_manual(run_type)`: `getattr(x, 'value', x)` — как в `plugins/utils.py` |
| Определение ручного рана не покрыто тестом; backfill без свидетеля | DS | тест `_manual` на `DagRunType.MANUAL/SCHEDULED/BACKFILL_JOB` и строках |
| Ветка авто-рана на границе в новом тесте | agy | добавлена рядом (уже была в `test_names_ts_and_yesterday_msk` — агент видел только дифф) |

## Стенд AF3

- `manual_today_7` и после правок `manual_today_8`: ручной ран без дат, src логов этого дага — `archive` success
  (за сегодня); тот же ран при логике «вчера» (`daily_runafter_6`) был skipped.
