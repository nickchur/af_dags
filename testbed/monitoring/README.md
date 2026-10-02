# 📈 Prometheus стенда

*2026-10-02 10:26 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

На сигме `/admin/metrics` вебсервера (`plugins/prometheus_exporter.py` etl-core) опрашивает
Prometheus. На стенде теперь тоже: контейнер `aftest-prometheus` раз в 30 с забирает
`/platform/app-dataplatform-etl-webserver/admin/metrics/`, хранит 7 дней.

- Адрес — только `127.0.0.1:9090` стенда (сеть хоста), снаружи — `ssh -L 19090:127.0.0.1:9090 testsrv`.
- Цель — `airflow-webserver`, метка `contour="stand"`.
- Развернуть: `scp prometheus.yml docker-compose.yml testsrv:/opt/aftest/monitoring/`, затем
  `docker compose up -d` там же.

Grafana не поднята: дашборды из `etl-core/grafana` строятся на `af_agg_*` с меткой
`airflow_id` (формат statsd-экспортёра), а `/admin/metrics` таких метрик не отдаёт — на
стенде они были бы пустыми.
