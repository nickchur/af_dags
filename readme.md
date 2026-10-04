# CTL — Change Tracking & Loading
*2026-10-04 12:07 MSK · v1.18 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Система автоматизированного управления ETL-процессами на базе **Apache Airflow** с интеграцией в **CTL API** и выполнением SQL-логики в **Greenplum**.

---

## Структура репозитория

```
ctl_worker/          # DAG'и Airflow
├── ctl_worker.py    # ⚙️ Динамическая генерация DAG'ов (1 на workflow): SQL в GP → публикация → retry
├── ctl_sensor.py    # 📡 Опрос CTL (1 мин): фильтрует активные загрузки и запускает DAG'и
├── ctl_loader.py    # 📥 Выгрузка метаданных CTL в S3 + Airflow Variables (workflows, сущности, события)
├── ctl_monitor.py   # 📊 Анализ загрузок (15 мин): SLA, retry, reStarted, Aborted
├── ctl_events.py    # 🔔 Публикация Dataset'ов CTL/{profile}/{eid}/{ename} для запуска зависимых DAG'ов
├── ctl_config.py    # 🔐 Инициализация конфигурации в Airflow Variable ctl_config (PIN-защита)
├── ctl_checker.py   # 🔍 Ручная диагностика CTL API: HTTP-запросы с шаблонами URL
├── ctl_yml.py       # 💾 Экспорт конфигурации CTL в YAML-файлы в S3 (бэкап / IaC)
├── ctl_tfs.py       # 📁 TFS → S3: по расписанию (tfs_sensor) и по Kafka-событию (tfs_kafka) с квитанцией
├── ctl_test.py      # 🧪 Симулятор: тестовые события / Dataset-сигналы / случайные триггеры
├── ctl_test_conn.py # 🔌 Мониторинг подключений (CTL, GP, PG, S3) с backoff
├── ctl_core.py      # 🧠 Ядро: retry, события (AND/OR), TIME-WAIT, нормализация данных (не DAG)
└── ctl_utils.py     # 🔧 API-обёртки, SQL, S3, конфигурация (get_config), логирование (не DAG)

s3_tools/                # S3-инструменты альфы (ручной запуск) → s3_tools/readme.md
├── s3_from_content.py   # 📤 Загрузка текстового контента в S3
├── s3_to_s3.py          # 📦 Копирование объекта между S3-бакетами
├── s3_to_s3_test.py     # 🔍 Поиск по маске и копирование/перемещение S3→S3
├── s3_checker.py        # 👁️ Просмотр файлов S3: маска, сортировка, чтение содержимого
├── s3_set_ttl.py        # ⏱️ Управление TTL-правилами S3-бакета
├── s3_bucket_list.py    # 📋 Список всех бакетов по всем S3-подключениям
├── s3_bucket_viewer.py  # 🪣 Список бакетов через HrpS3BucketViewerOperator
└── s3_viewer.py         # 🗂️ Список ключей и чтение файлов через HrpS3*Operator

tools/                   # Служебные DAG'и: проверка и обслуживание → tools/readme.md
├── test_connections.py  # 🔎 Список подключений secret backend и проверка доступности каждого
├── test_hrp_operators.py # 🧪 Функциональный стенд для hrp_operators (pg↔s3↔ch)
├── test_kafka.py        # 📨 Проверка Kafka: продюсер и консьюмер тестовых сообщений
├── test_dags.py         # 🧬 Проверка сериализации DAG'ов, снимки версий
├── db_cleanup.py        # 🧹 Очистка метадаты Airflow старше N дней
├── log_cleanup.py       # 🪣 Обслуживание бакета логов задач: удаление старых объектов
├── log_events.py        # 📊 Сбои доставки задач: отчёт по журналу метабазы
├── system_health.py     # 🩺 Состояние контура: пульс раз в 5 мин и снимок раз в час
├── pg_activity.py       # 🐘 Сторож метабазы: зависшие сессии, долгие запросы, блокировки
├── queue_analyze.py     # 🔬 Разбор очереди: почему задачи ждут; чистка брокера по галочке
├── paused_runs_cleanup.py # ⏸️ Зависшие раны запаузенных дагов: отчёт и Mark failed
├── mcp_skills.py        # 🧭 Навыки агента и документация дагов → Variables для MCP
├── dummy.py             # 🫀 Раз в час: шедулер и воркер живы, задержки планирования и очереди
└── skill/tools.md       # 🤖 Навык агента: служебные даги

gp_exchange/                 # Приём универсального обмена из ПКАП: S3 → ClickHouse
├── tfs_exchange_sensor.py   # 📡 S3KeySensor на ue_exchange_*.csv, публикует Dataset
├── tfs_exchange_import.py   # 📥 Загрузка CSV в gp_ue_exchange, ветвление по _gp_name, разбор JSON в целевые таблицы
└── tfs_exchange_common.py   # ⚙️ Конфигурация тракта ТФС (сценарий, бакет, топик) и сообщение TransferFileCephRq

plugins/             # Переиспользуемые модули (импортируются DAG'ами)
├── tfs_utils.py     # 🚚 Конфигурация, утилиты и хранилище тракта Kafka ↔ ТФС
├── s3_utils.py      # ☁️ Расширенные S3-утилиты: TTL, копирование, ZIP-распаковка
└── utils.py         # 🛠️ Общие хелперы Airflow: пулы, заметки, колбэки, timedelta

GP/                  # 🐘 Снимок DDL Greenplum: то, что тракт CTL вызывает и куда пишет
├── srv_wf/          # Точка входа pr_swf_start_ctl, логирование pr_log_ctl, движок, отчёты
└── srv_dq/          # ККД: Z-тест (pr_ztest_set / pr_ztest_all_diff) и его конфигурация

testbed/             # 🧰 Тестовый стенд, не DAG'и: из разбора Airflow убран .airflowignore
├── ctl_worker/      # 🎭 Эмулятор CTL API и сборщик фикстур из снимка edpetl-ctl
├── gp_exchange/     # 🐘 Greenplum на PostgreSQL: тракт обмена ПКАП целиком
└── vault/           # 🔐 make_vault.py — эмуляция /vault/secrets/application
```

`GP/` — копия из `HR_Data` для чтения, а не источник истины; контракт вызова, коды
результата и карта объектов описаны в [`GP/readme.md`](GP/readme.md).

---

## Как работает система

1. **`ctl_loader`** (каждые 5 мин, `loader_interval`) — выгружает из CTL метаданные: workflows, сущности, события — кладёт в Airflow Variables и S3 (папка `ctl/` бакета логов).
2. **`ctl_sensor`** (каждую минуту) — опрашивает CTL, фильтрует активные загрузки (`RUNNING`, `TIME-WAIT`, `EVENT-WAIT`), запускает нужные DAG'и.
3. **`ctl_worker`** (per workflow) — выполняет цикл:
   - `run_prm` → инициализация загрузки в CTL
   - `run_exe` → выполнение SQL в Greenplum
   - `run_val` → публикация статистики
   - `run_sts` → решение: `success / retry / error`
   - `run_end` → финальный статус
4. **`ctl_monitor`** (периодически) — проверяет SLA, при нарушениях переводит загрузки в `ABORTED` или инициирует перезапуск.

### Жизненный цикл загрузки

```
INIT → RUNNING → SUCCESS → COMPLETED
              ↘ ERRORCHECK → TIME-WAIT → RUNNING (retry)
              ↘ ABORTED
EVENT-WAIT ──→ RUNNING
```

### Коды результата (`res`)

| Код | Значение | Действие |
|-----|----------|----------|
| `> 0` | Успех | `SUCCESS` |
| `0` | Нет данных | `SUCCESS` (no) |
| `-7` | Циклический retry | повтор |
| `< 0` | Ошибка | retry или `ABORTED` |

---

## Подключения

| Система | Connector ID | Тип |
|---------|-------------|-----|
| CTL API | `ctl` | KerberosHttp |
| Greenplum | `alpha-adb_dev_comm-read` | Postgres |
| S3 | `s3` | S3 |
| Airflow DB | `airflowdb` | Postgres |

---

## Запуск

```bash
# 1. Настроить подключения в Airflow UI

# 2. Запустить DAG инициализации конфигурации
CTL.<profile>.config

# 3. Активировать загрузчик метаданных
CTL.<profile>.loader

# 4. Активировать сенсор событий
CTL.<profile>.sensor
```

---

## Зависимости

- Apache Airflow 2.10.1
- Greenplum 6.x / psycopg2
- boto3 (S3)
- tenacity (retry)
- pendulum
- PyYAML
- hrp_operators (KerberosHttpHook)

---

## Контекст для работы

| Файл | О чём |
|---|---|
| [CLAUDE.md](CLAUDE.md) | правила работы в репозитории: язык, метки версий, контракты, порядок правки поведения |
| [CONTEXT.md](CONTEXT.md) | карта артефактов и их свежесть — собирается автоматически |

После `pull`, `merge` и `checkout` git-хуки сами раскладывают навыки и команды агента,
пересобирают карту и показывают документы, отставшие от кода. Включаются один раз на клон:

```bash
bash .githooks/install.sh
```

---

## Спецификации

Каждый каталог репозитория — отдельный проект со своей baseline-спецификацией в
`docs/sberpowers/specs/`: что система обязана делать — требования `REQ-<возможность>-NN`,
критерии приёмки, ограничения и происхождение шрамов. Общий контекст (стек, контуры,
соглашения) — в `openspec/project.md`.

| Каталог | Спецификация |
|---|---|
| `ctl_worker/` | `docs/sberpowers/specs/ctl-worker-baseline.md` |
| `plugins/` | `docs/sberpowers/specs/plugins-baseline.md` |
| `er_export/` | `docs/sberpowers/specs/er-export-baseline.md` |
| `tfs_kafka/` | `docs/sberpowers/specs/tfs-kafka-baseline.md` |
| `xs_export/` | `docs/sberpowers/specs/xs-export-baseline.md` |
| `s3_tools/` | `docs/sberpowers/specs/s3-tools-baseline.md` |
| `tools/` | `docs/sberpowers/specs/tools-baseline.md` |
| `gp_exchange/` | `docs/sberpowers/specs/gp-exchange-baseline.md` |

Спека описывает требуемое поведение, а не текущее состояние кода: расхождение между ними —
это дефект, который видно сравнением, а не повод переписать спеку. Так были найдены и
закрыты два расхождения в `gp-exchange` — первая загрузка потока и публикация события на
пустом ветвлении; оба проверены прогоном на стенде (`testbed/gp_exchange/`).

Процесс — SberPowers: изменение поведения начинается с дельта-спеки
`docs/sberpowers/specs/ГГГГ-ММ-ДД-<изменение>.md` (ADDED / MODIFIED / REMOVED) и плана в
`docs/sberpowers/plans/`; после реализации дельта вливается в baseline. Формат baseline
проверяет `python3 .claude/scripts/check_baseline.py`. До 04.10.2026 спецификации велись в
OpenSpec — их история в `openspec/changes/archive/`. Readme отвечает на «как устроено»,
спека — на «что обязано работать»; дублировать одно в другом не нужно.

---

**Автор:** EDP.ETL | **Версия:** 1.2 | **Год:** 2026
