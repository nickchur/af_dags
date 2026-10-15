# Discover: оператор загрузки S3 → GP/PG (CSV и JSON)

- Дата: 2026-10-15
- Зона изменения: af_dags `plugins/` (первый этап), затем пакет etl-core `hrp_operators/` (второй этап —
  решение пользователя 15.10.2026: «сначала af_dags, потом в etl-core»)

## Карта зоны изменения

- `ctl_worker/ctl_worker.py:786-900` — таск `run_tfs`: поток ТФС → GP (`wf_tfs_in`), маска, `.done`,
  таблица из имени файла, усечение раз на таблицу, перенос в `tfs-archive` / `tfs-errors`, статусы CTL.
- `ctl_worker/ctl_utils.py:479` `gp_upload_s3_csv` — нынешний загрузчик: S3 → (zip | gz | как есть) → `gp_from_stream`.
- `ctl_worker/ctl_utils.py` `gp_from_stream` — `COPY {schema}.{table} FROM STDIN WITH {options}`,
  `autocommit`, `statement_timeout`, `TRUNCATE` перед загрузкой.
- `tools/test_json_pg.py` — тестовый даг (15.10.2026): потоковый разбор JSON-массива и конверта
  `{"data": [...]}`, COPY пачками в `jsonb`, один код на PG/GP; проверен на стенде.
- etl-core `hrp_operators/s3_to_postgres.py` — `HrpS3ToPostgresOperator2`: только CSV, инкарнации.
- etl-core `hrp_operators/utils/pg_utils.py` — инкарнации: `current_incarnation`, `truncate_incarnation`,
  `activate_incarnation` (последовательность `<table>_inc_seq`).
- etl-core `hrp_operators/utils/compress_utils.py` — `COMPRESS_EXT_MAP`, `UNCOMPRESSOR_MAP` (gzip, zip).
- etl-core `local_settings/airflow_local_settings.py:180` + `local_settings/allowed_operators.txt` — белый список операторов.
- etl-core `plugins/operators_ru.md` — русские описания операторов (страница Docs → Operators, MCP `get_allowed_operators`).
- Тесты: af_dags — без pytest, проверки в `testbed/` и самопроверки модулей, живьём — стенд;
  etl-core — `tests/` (pytest), регрессия операторов — даг `tools/test_hrp_operators.py` (af_dags).

## Факты

- `HrpS3ToPostgresOperator2` грузит только CSV: `COPY … FROM STDIN WITH CSV QUOTE '<quote>'`,
  кавычка по умолчанию `\x01` — `s3_to_postgres.py:33, 230-232`.
- Он же коммитит промежуточно по времени и объёму буфера — `s3_to_postgres.py:40-46, 118-119, 268-272`.
- Пишет в неактивную инкарнацию `<table>_<0|1>` и переключает `setval('<table>_inc_seq')` —
  `s3_to_postgres.py:213, 353-358`; `pg_utils.py:18-57`.
- Колонка с именем файла — `include_file_name`, `file_name_position` — `s3_to_postgres.py:57-60, 423-436`.
- Сжатие: `compression` = `None` | `gzip`; ключ ищется и с расширением сжатия — `s3_to_postgres.py:55, 140-162`.
- Получение хука по conn_id — `hook_utils.py:11` (`connection.get_hook()`).
- Нынешний поток ТФС → GP — CSV через `gp_upload_s3_csv`: zip (имена cp866, несколько файлов в архиве,
  усечение раз на архив), gz, без сжатия — `ctl_utils.py:479-530`. Опции COPY по умолчанию
  `CSV HEADER DELIMITER ','` — там же.
- `gp_from_stream` — одна транзакция на файл (`autocommit`, COPY одной командой), `statement_timeout`
  из `gp_timeout(wf_prm)` — `ctl_utils.py`, `ctl_worker.py:807, 863`.
- Схема по умолчанию `s_grnplm_vd_hr_edp_dia`, префикс добавляется, если не указан — `ctl_utils.py`, `ctl_worker.py:809-811`.
- Тестовый файл ИФТ альфы `SCPL_to_SIGMAPKAP_20261012_124000646.json` (поток tfs2gp): объект
  `{"data": [...]}`, элемент `{"schema": {...}, "payload": {...}}` (конверт Kafka Connect), схемы
  элементов разные (4 элемента — 48 разных полей payload); копия в R2 `test/sber-docs/inbox/2026-10-15/`.
- Политика кластера: `if task.task_type not in ALLOWED_OPERATORS: raise AirflowClusterPolicyViolation`
  — etl-core `local_settings/airflow_local_settings.py:180-182`; сравнение по имени класса.
- Политика едет только в образы сигмы: `COPY local_settings/airflow_local_settings.py …` —
  `HRP-pipeline/app-dataplatform-etl-worker/Dockerfile:138-139`, `…/dag-processor/Dockerfile:116-117`.
  В репозитории альфы (`etl-core-alpha/app-dataplatform-etl-alpha/etl-core/`, `src/`) ни
  `airflow_local_settings.py`, ни `allowed_operators.txt` нет.
- Права GP (со слов пользователя 15.10.2026): `cap_gp_etl` / `-read` — всё, кроме ALTER и DROP, на
  схемы `*_vd` / `*_vda` прав нет; `-write` (`cap_gp_ppl`) — владелец. Слои `dia → stg`, сырьё — `*_dia`.

## Ограничения и риски

- **Белый список сигмы.** Оператор из af_dags `plugins` на сигме уронит даг на разборе: имени класса
  нет в `allowed_operators.txt`. Первый этап годится только для альфы; на сигму — после переноса в
  etl-core и строки в списке (`airflow_local_settings.py:180`).
- **Длительность транзакции.** Оператор — для баз, где пишущая транзакция не ограничена или предел
  несущественен (пользователь 15.10.2026); загрузка потоковая. На GP предел 4,5 часа (сервер рвёт по
  правилу «Query Time 4,5H (GLOBAL)» вместе с соединением — `statement_timeout` держат на ~5 минут ниже,
  `gp_exe`). Нынешний `gp_from_stream` грузит файл одной транзакцией со `statement_timeout` из `gp_timeout`.
- **Усечение и блокировка.** `TRUNCATE` берёт ACCESS EXCLUSIVE до конца транзакции: при загрузке одной
  транзакцией читатели таблицы ждут всю загрузку. `DELETE` читателей не держит, но оставляет мёртвые
  строки. Готовый выбор между ними — `s_grnplm_vd_hr_edp_srv_wf.pr_soft_truncate(srs)`: `lock … NOWAIT` +
  `truncate`, при `lock_not_available` — `delete` (HR_Data `sql/create/s_grnplm_vd_hr_edp_srv_wf/functions/pr_soft_truncate.sql`).
  Схема `*_vd_*` — у `-read` прав может не быть, на PG функции нет.
- **Атомарность загрузки.** Промежуточные коммиты делают частичную загрузку видимой; у инкарнаций
  это закрыто переключением после конца, у прямой загрузки в таблицу — нет.
- **Поток ТФС → GP на альфе живой** (`run_tfs`): контракт — таблица из имени файла, `wf_tfs_options`,
  `wf_tfs_truncate`, архив/ошибки, статусы CTL; замена загрузчика не должна его менять.
- **Синтаксис GP6.** Стенд на PostgreSQL 16 не подтверждает: `COPY … WITH (…)`, `jsonb`-функции,
  `DISTRIBUTED`, приоритет `IS` над `->>` (`reference_gp6_operator_precedence`).
- **Python 3.10–3.12, без новых зависимостей**: `ijson` в образах нет; `stream_unzip` есть (`ctl_utils.py`).
- **Двойной импорт plugins** с fallback на `CI06932748.tools` — модуль `plugins/` едет и на сигму.

## Открытые вопросы

### Найду сам
- Принимает ли GP6 `COPY … WITH (FORMAT csv, QUOTE …)` и `jsonb_each` — нужен прогон на контуре
  (тестовый даг `tools_test_json_pg` годится как проба).
- Используется ли `HrpS3ToPostgresOperator2` в дагах контуров и есть ли инкарнационные таблицы в GP.
- Какие ещё форматы приходят по `wf_tfs_in` на альфе (маски, расширения) — по Variable `ctl_workflows`.

### Для grill
- Новый оператор или расширение `HrpS3ToPostgresOperator2` (во втором этапе)? Имя: `HrpS3ToGPOperator`
  при поддержке и PG.
- Режим записи: прямо в таблицу (+ `truncate`), инкарнации или оба.
- Транзакции: одна на файл (атомарно, как `gp_from_stream`) или коммиты по времени/объёму.
- JSON: строка в одну колонку `jsonb` (раскладка — SQL дальше по слоям) или раскладка по колонкам
  целевой таблицы в Python? Что брать из конверта Kafka Connect — `payload`, весь элемент, `schema`?
- Набор форматов: CSV (опции COPY), JSON-массив, массив под ключом, JSON Lines; сжатие gz/zip
  (несколько файлов в архиве).
- Служебные колонки: имя файла, время загрузки, номер элемента.
- Где граница оператора: он только грузит, или ещё маска/несколько файлов/архив и `tfs-errors`/`.done`?
- Подключать ли оператор к `run_tfs` в этой же задаче или отдельной.
