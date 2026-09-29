## ADDED Requirements

### Requirement: Снимки CTL — в папке ctl/ бакета логов

`ctl_obj_save`, `ctl_obj_load` и `ctl_obj_etag` SHALL работать с ключами `ctl/<ключ>.<ext>`
бакета логов, подключением логов (`[logging] remote_log_conn_id`). Своей настройки S3 у CTL
SHALL NOT быть. Чтение SHALL сначала брать Variable и идти в S3 только без неё.

#### Scenario: Снимок загрузки

- **WHEN** воркер пишет `ctl_working/5`, а бакет логов — `s3://bucket/logs`
- **THEN** объект ложится в `bucket` под ключом `ctl/ctl_working/5.json`
