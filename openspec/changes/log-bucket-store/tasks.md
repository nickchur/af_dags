## 1. Код
- [x] 1.1 `tools/log_cleanup.py`: один срок и правило на корень, снятие правил на папки
- [x] 1.2 `tools/test_dags.py`: освежение последней версии старше 7 дней
- [x] 1.3 `tools/mcp_skills.py`: документы в `docs/` бакета логов, `at` в оглавлении, без `store_docs`
- [x] 1.4 `plugins/ctl_utils.py`, `ctl_worker/ctl_config.py`: снимки CTL в `ctl/` бакета логов
- [x] 1.5 `tfs_kafka_rcv`/`tfs_kafka_snd`: RqUID в заметках
- [x] 1.6 Навыки `tfs-kafka`, `ctl-worker`; readme; частота `ctl_loader`

## 2. Проверка
- [x] 2.1 Стенд 25.09: `ctl_obj_save` → `ctl/zz_lb_check.json` в бакете логов, `ctl_obj_etag`, `ctl_obj_load` из S3, повторная запись — MD5 match
- [x] 2.2 Стенд 25.09: `sweep` с `dry_run` — 6 папок, логи последними; `layout` — правило `DeleteAfter` 120 дн. на корень, 6 правил на папки сняты
- [x] 2.3 Стенд 25.09: `snapshot_dags` — у `tools_log_cleanup/00004` (16.09) дата обновилась, номер прежний, 00002/00003 не тронуты
- [x] 2.4 Стенд 25.09: `publish_docs` — 12 документов в `docs/`, повтор без записи

## 3. После выкладки
- [ ] 3.1 Дев: `days=30` через `save_params`
- [ ] 3.2 Сигма: `purge_docs` после выкладки etl-core 1.1.32
