Axis-A: соответствует (REQ не меняется: письма с задач были выключены)
Axis-B: принято
Tests: стенд AF3 — DAG с @task.sensor и default_args из plugins/utils.py: предупреждений про email 0 (было 3); dag-processor перечитал ctl_worker/* без ошибок импорта

# Ревью: убраны email-ключи из default_args (предупреждения AF3 в логе задачи)

| Находка | Кто | Решение |
|---|---|---|
| Находок нет | DeepSeek v4-pro, agy gemini-3.8-flash-high | — |
| `except ImportError` в otel-плагине без WARNING скрывает и нештатный сбой импорта | DeepSeek (необязательное) | Отклонено: остаётся INFO «engine not available» — диагностика есть, другой причины ImportError у `airflow.settings.engine` нет |
