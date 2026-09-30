## ADDED Requirements

### Requirement: Импорт общих модулей не ходит в сеть и не требует Airflow Variables

Модули `plugins` SHALL выполнять чтение конфигурации, списка бакетов и любые сетевые
обращения только при первом фактическом вызове, а не на импорте. Парсинг DAG-файла с
недоступной базой, отсутствующей Variable или неработающим S3 MUST завершаться успешно.

#### Scenario: S3 недоступен при разборе DAG'а

- **WHEN** Airflow разбирает DAG, импортирующий модули `plugins`, а S3 не отвечает
- **THEN** импорт проходит; S3 понадобится только первому вызову, который в него пишет или читает

## REMOVED Requirements

### Requirement: Импорт модуля не ходит в сеть и не требует Airflow Variables

**Reason**: сценарии требования были про `get_config` и `ctl_config` — это модуль CTL, он переехал в `ctl_worker/`.
**Migration**: для общих модулей — «Импорт общих модулей не ходит в сеть…» здесь же; для модулей CTL — «Импорт модулей CTL не ходит в сеть…» в `specs/ctl-worker`.

### Requirement: Решатели возвращают статус, а не бросают исключение

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Обращения к CTL API ограничены по темпу и повторяются при обрыве

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Запросы к Greenplum и PostgreSQL переживают обрыв соединения

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Объекты читаются из Variable, а хранятся в S3

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Конфигурация повторов восстанавливается из нескольких источников

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Ожидание событий по стратегиям AND и OR

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Единая проверка доступности подключений

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Усечение таблицы — одно на объект S3

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Неразобранная часть дельты — ошибка, а не тишина

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

### Requirement: Снимки CTL — в папке ctl/ бакета логов

**Reason**: модули `ctl_core` и `ctl_utils` переехали из `plugins/` в `ctl_worker/` — кроме дагов CTL их никто не импортирует.
**Migration**: требование без изменений перенесено в `specs/ctl-worker`.

