# Движок srv_wf и отчёты CTL на стенде (PostgreSQL)
*2026-09-26 19:55 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Настоящий движок загрузок (`pr_swf_start_ctl` и его журналы) и отчёты CTL (`pr_mail_*`,
`pr_check_*`) в базе `gp_test` тестового стенда. Зачем: стенд работает постоянно в тестовом
режиме (`test_mode`), и отчёты там строятся по-настоящему (`test_real` в `ctl_config`) поверх
журнала, который наполняют тестовые прогоны. На этих данных проверяются навыки агента
`ctl-worker` и `ctl-reports` и MCP-инструменты `ctl_workflow`/`ctl_report`.

## Как устроено

- Источник — снимок боевого SQL в `GP/` (`srv_wf`, `srv_dq`). Его не правим: `build.py` берёт
  файлы как есть и механически убирает то, чего нет в PostgreSQL, — `DISTRIBUTED …`,
  `WITH (appendonly…)`, `EXECUTE ON ANY`, а также `COMMENT ON` (описания стенду не нужны, а в
  части файлов они с дефектом, см. ниже). Всё остальное — боевой код.
- `05_stubs.sql` — подготовка: схемы, последовательности журналов (их DDL в HR_Data нет),
  замена таблиц-двойников стенда обмена (`testbed/gp_exchange/10_schema.sql`) настоящими
  вьюхами с теми же колонками, переименование журнала заглушки `tb_log_ctl` в
  `tb_log_ctl_mock_old` (другая структура; журналы не удаляем).
- `deploy.sh` — сборка, бэкап схем (`/opt/aftest/gp_test-srv-backup-*.sql`), прогон, пересоздание
  вьюх обмена, которые уходят каскадом.

```bash
bash testbed/gp_engine/deploy.sh
```

## Что работает

| Объект | Проверено |
|---|---|
| `pr_swf_start_ctl` + `vw_swf_ctl_log` | тестовый `exe` даёт `res = 1`, ответ виден в журнале — повтор `run_exe` находит его (`gp_loading_result`) |
| `pr_log_ctl` → `tb_log_ctl` → `vw_log_ctl_*` | ответы эмулятора CTL журналируются, вьюхи отчётов их читают |
| `pr_mail_ctl_status` | HTML отчёта |
| `pr_mail_ctl_alerts` | «no new alerts», пока у потоков нет `wf_alert` |
| `pr_check_ctl` / `pr_check_etl` | развёрнуты: без них `pc1080.check_sberchat` в `test_real` падал бы с `-7` |

Потоки-отчёты эмулятора — `testbed/ctl_worker/workflows_extra.json` (в снимке с боя их нет):
`pc1080.mail_ctl_status` (каждые 2 ч), `pc1080.mail_ctl_alerts` (каждые 30 мин), расписание
строит Airflow (`scheduled: false`, режим `mixed`), даги создаются на паузе.

## Чего нет и почему

- `pr_mail_ctl_report` (блокировки через `pg_stat_activity`/`pg_locks` GP),
  `pr_mail_ctl_work_load_report` (skew, resgroup — только в GP), `informatica`, `sdpue`,
  `ztest` — следующим заходом.
- `COMMENT ON` вырезаются: в `GP/srv_wf/views/vw_log_ctl_entity.sql` (HR_Data E360-5966)
  неэкранированные кавычки внутри строки комментария (`obj = 'entity'`) — файл падает и в GP.
