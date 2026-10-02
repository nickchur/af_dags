# 🎭 Эмулятор CTL API для тестового стенда

*2026-10-02 14:53 MSK · v1.7 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

`ctl_worker/` — единственный каталог репозитория, который до сих пор проверялся только
выкладкой на alpha: все его даги ходят в CTL API, а он живёт на контуре и закрыт Kerberos.
Эмулятор отвечает так же, как CTL, и держит состояние загрузок — поэтому полный цикл
`run_prm → run_exe → run_end` гоняется на стенде, вместе с ретраями и ветками ошибок.

## Из чего состоит

| Файл | Что делает |
| :--- | :--- |
| `ctl_mock.py` | сам эмулятор: starlette + psycopg2, порт 9080 (у AF3 — 9081) |
| `schema.sql` | схема `ctl_mock` в стендовом postgres и заглушки `pr_swf_start_ctl` / `pr_log_ctl` |
| `fixtures_from_cache.py` | снимок бакета `edpetl-ctl` → фикстуры эмулятора |
| `ctl-mock.service` | systemd-юнит |

Справочники (воркфлоу, категории, сущности, профиль) — фикстуры, только чтение.
Состояние (загрузки, статусы, параметры, statval'ы, расписание воркфлоу) — в postgres,
база `adb_dev_comm`, схема `ctl_mock`.

## Kerberos

С 02.10.2026 эмулятор на стенде AF2 закрыт Kerberos, как настоящий CTL: задан
`CTL_MOCK_KERBEROS_KEYTAB` (keytab с `HTTP/ctl-mock.stand`, KDC — [`testbed/kdc`](../kdc/README.md)),
подключение `ctl` смотрит на `ctl-mock.stand`. Запрос без билета получает `401` с
`WWW-Authenticate: Negotiate`, `KerberosHttpHook` повторяет его с токеном из кэша
`airflow kerberos` — та же ветка кода, что на альфе. В журнале `ctl_mock.api_log` это пара
`401` → `200` на каждый новый сеанс.

Без переменной (так работает `ctl-mock3` для AF3) авторизации нет вовсе:
`KerberosHttpHook` ставит `HTTPKerberosAuth(mutual_authentication=OPTIONAL)` без
`force_preemptive`, а requests-kerberos генерирует токен только в ответ на `401` — эмулятор
его тогда не отдаёт, KDC и keytab не нужны.

## Почему не нужен Greenplum

`run_exe` всегда зовёт `select pr_swf_start_ctl(...)`, но при `test_mode` подставляет в
`exe` безобидное `'Ok Test work', pg_sleep(N)`. Заглушка `pr_swf_start_ctl` выполняет
ровно то, что пришло в `exe`, и разбирает ответ теми же правилами, что боевая:

| Ответ выражения | `res` | Что делает воркер |
| :--- | :--- | :--- |
| пусто или `ok …` | `1` | SUCCESS |
| `no …` | `0` | нет данных |
| исключение | `-7` | ERROR, циклический retry |
| таймаут оператора | `-2` | ERROR |
| что-то иное | `-9` | ERROR |

Значит сценарии ошибок задаются параметром `wf_exe` у воркфлоу, а не правкой кода.

Со включённым `test_mode` до `wf_exe` дело не доходит: `run_exe` подменяет `exe` на
ожидание (верхняя граница — `test_sleep`, на стенде ставим `minutes=2`) и подставляет код
результата по профилю. Профиль `ok-no-error` даёт и успех, и «нет данных», и ошибки,
включая `-7` с уходом в TIME-WAIT, — то есть весь разбор кодов гоняется без правки
параметров воркфлоу. Нужен конкретный сценарий (например, деление на ноль в GP) — ставьте
`"test_mode": "off"` и задавайте `wf_exe`.

⚠️ Тестовый режим работает только на контурах DEV и IFT, симулятор — на DEV, IFT и PSI, а
событийный режим симулятора — только на DEV. Контур берётся из `ENV_STAND`; на стенде она
выставлена в `DEV` (`/opt/aftest/airflow.env`), так что оба режима доступны. Снимите
переменную — и стенд начнёт вести себя как бой: симулятора не будет вовсе, а тестовый
режим станет игнорироваться с заметкой.

## Разворачивание

```bash
# 1. Снимок бакета и фикстуры (25 — сколько воркфлоу оставить, см. ниже)
unzip -q edpetl-ctl.zip -d /tmp/ctlsnap
/opt/aftest/venv/bin/python fixtures_from_cache.py /tmp/ctlsnap/edpetl-ctl /opt/aftest/ctl-mock/fixtures 25

# 2. Схема состояния и заглушки GP
docker exec -i aftest-postgres psql -U airflow -d adb_dev_comm < schema.sql

# 3. Сервис
cp ctl-mock.service /etc/systemd/system/ && systemctl daemon-reload && systemctl enable --now ctl-mock
curl -s http://127.0.0.1:9080/v5/api/info
```

⚠️ **Сколько воркфлоу оставлять.** `ctl_worker.py` строит по DAG-у на каждый воркфлоу
профиля. В снимке их 685 — разбор файла не укладывается в `dagbag_import_timeout` (30 сек),
и стенд не получает ни одного дага воркера. Для тракта хватает пары десятков; в набор
всегда попадают воркфлоу тех загрузок, что приехали из снимка.

## Второй экземпляр — для AF3

С общим эмулятором ран AF3 двигал загрузки и события стенда AF2, поэтому у AF3 свой
экземпляр того же кода. Общие у двух стендов только MinIO, ClickHouse и Kafka.

| | AF2 | AF3 |
| :--- | :--- | :--- |
| Каталог и юнит | `/opt/aftest/ctl-mock`, `ctl-mock` | `/opt/aftest/ctl-mock3`, `ctl-mock3` |
| Порт | 9080 | 9081 |
| База состояния и GP | `gp_test` | `gp_test3` |
| Подключения vault | `/vault/secrets/application` | `/vault/secrets/application3` (`ctl` → 9081) |
| Бакеты CTL | `edpetl-ctl`, `edpetl-files` | `edpetl-ctl3`, `edpetl-files3` |
| Бакет логов | `hrplt-test` | `hrplt-test3` |

```bash
# База — копия gp_test; номера загрузок сдвинуты, чтобы не совпадать с AF2 в общих бакетах и логах
docker exec aftest-postgres createdb -U airflow gp_test3
docker exec aftest-postgres sh -c 'pg_dump -U airflow gp_test | psql -qU airflow gp_test3'
docker exec aftest-postgres psql -U airflow -d gp_test3 -c "select setval('ctl_mock.loading_id_seq', 93000000)"

# Каталог — копия ctl-mock; в ctl-mock.env база gp_test3, в юните порт 9081 и каталог ctl-mock3
cp -a /opt/aftest/ctl-mock /opt/aftest/ctl-mock3
sed -i 's#/gp_test$#/gp_test3#; s#ctl-mock/#ctl-mock3/#' /opt/aftest/ctl-mock3/ctl-mock.env
sed 's#ctl-mock#ctl-mock3#g; s#9080#9081#' ctl-mock.service > /etc/systemd/system/ctl-mock3.service
systemctl daemon-reload && systemctl enable --now ctl-mock3
curl -s http://127.0.0.1:9081/v5/api/info
```

Метабаза AF3 своя, поэтому в ней же заводятся `alpha-adb_dev_comm-read` / `-write` на
`gp_test3`, а `ctl_config` AF3 смотрит на `http://127.0.0.1:9081` и бакеты `*3`. Файл vault
AF3 выбирает `scripts/stand_af3_env.sh` из etl-core-3.

## Что нужно в Airflow

| Что | Значение на стенде |
| :--- | :--- |
| Подключение `ctl` | в payload `HTTP_CONNECTIONS`: `{"schema": "http", "host": "127.0.0.1", "port": 9080}` |
| Подключения `gp` / `ppl` | `alpha-adb_dev_comm-read` / `-write` → postgres `adb_dev_comm` |
| Подключение `pg` | `airflowdb` → метабаза |
| Variable `ctl_config` | копия `ctl_config.json` с этими conn_id, `simulator: "event"`, `test_mode: "ok-no-error"` и бакетами `edpetl-ctl` / `edpetl-files` |
| Пулы | `ctl_pool`, `gp_pool` — без них таски висят в очереди |
| Бакеты MinIO | `edpetl-ctl`, `edpetl-files` |

## Две вещи, в которых эмулятор намеренно повторяет CTL

**Список и полная загрузка отдают РАЗНОЕ.** `GET /v4/api/loading/{lid}` возвращает полный
объект (`startCondition`, `retriesLeft`, `awaitedEvents`, `workflow`, `locksSet`), а список
`/v5/api/loading/extended` — только ядро (`id`, `wf_id`, `profile`, `alive`, `auto`,
`status`, `status_log`, `start_dttm`, `end_dttm`, `params`, `stats`, `loading_status`,
`uuid`). Проверено по снимку бакета: у записей `ctl_prf_events` остальных ключей нет вовсе.
Эмулятор режет список так же (`EXTENDED_FIELDS` в `ctl_mock.py`) — иначе код, который
дотягивает условие запуска отдельным запросом, на стенде никогда бы не сработал.

**Время — московское.** Соединения эмулятора выставляют `SET TIME ZONE Europe/Moscow`:
в этой зоне CTL отдаёт `effective_from`, `satisfiedDttm` и прочие отметки, и её же ждёт
наш код (`ctl_config.tz`). Без этого стенд писал бы UTC, всё выглядело бы на три часа
старше, и пороги монитора (15 минут, 6 часов, SLA) проверялись бы неправдой — на этом
уже один прогон дал ложный `reStarted`.

⚠️ Фильтр `category_ids` эмулятор не применяет: монитор обходит категории и на каждой
видит один и тот же набор загрузок. На решения это не влияет, но запросов в журнале
получается в разы больше, чем было бы на контуре.

## Контракт загрузки, который легко пропустить

CTL кладёт в `params` загрузки два ключа, без которых наши даги её не берут:

| Ключ | Что будет без него |
| :--- | :--- |
| `loading_id` | `ctl_sensor` отбрасывает загрузку с пометкой «no loading_id» — молча, в заметке |
| `wf_id` | `ctl_add_chk` падает `KeyError: 'wf_id'` (`ctl_sensor.py:303`) |

Эмулятор проставляет оба при создании загрузки и туда же копирует параметры воркфлоу.
У загрузок из снимка их нет — поэтому сенсор их пропускает, ровно как в бою.

Ещё сенсор не берёт загрузку со статусом `RUNNING` и НЕПУСТЫМ `status_log`: это признак,
что её уже ведёт чей-то ран Airflow. Поэтому свежесозданной загрузке статус ставится
пустым логом: `PUT /v4/api/loading/{lid}/status {"status": "RUNNING", "log": ""}`.

## Сценарии

**Загрузчик.** `CTL.HR_Data.loader` собирает метаданные и раскладывает их в S3 и Variables
ровно так же, как на контуре: `ctl_workflows`, `ctl_categories`, `ctl_entities`,
`ctl_events`. Повторный запуск уходит в скип по контрольной сумме.

**Полный цикл.** Загрузка создаётся запросом к эмулятору:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/wf/<wf_id>/loading?scheduleAfterStart=false' \
     -H 'Content-Type: application/json' -d '{"wfp_run_type": "NO-WAIT"}'
```

Эмулятор ставит новой загрузке START и дальше её не двигает; сенсор берёт только RUNNING,
TIME-WAIT и EVENT-WAIT, а монитор перезапускает START лишь через час (`new_grace`). Поэтому
в RUNNING её переводят руками, и лог обязан быть пустым: RUNNING с логом сенсор считает уже
запущенным и пропускает.

```bash
curl -s -X PUT 'http://127.0.0.1:9080/v4/api/loading/<lid>/status' \
     -H 'Content-Type: application/json' -d '{"status": "RUNNING", "log": ""}'
```

Дальше `ctl_sensor` (раз в минуту) её видит и поднимает даг воркера. Смотреть глазами:

```sql
select id, alive, status, status_log from ctl_mock.loading order by id desc limit 5;
select status, log, effective_from from ctl_mock.loading_status
 where loading_id = <lid> order by effective_from desc;
select method, path, status from ctl_mock.api_log order by id desc limit 20;
```

**Ветка ошибки.** Тому же воркфлоу задаётся `wf_exe`, возвращающее не `ok`:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/wf/<wf_id>/params' \
     -H 'Content-Type: application/json' -d '{"wf_exe": "1/0"}'   # деление на ноль → res -7
```

**Событие (EVENT-WAIT).** Затравка statval'ов датирована вчерашним днём, поэтому
событие считается ненаступившим. «Наступление» — публикация свежего значения:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/loading/0/entity/<eid>/stat/2/statval?profile=HR_Data' \
     -H 'Content-Type: application/json' -d '["done"]'
```

**Висячие ссылки на события.** На alpha 14.09.2026 девять workflow ждали события
сущностей и профилей, которых в CTL нет, и CTL отвечал на их статистику 422. Снимок сам не
знает, чего в CTL нет, поэтому это задаётся переменными окружения эмулятора (в
`ctl-mock.env`):

| Переменная | Что делает |
| :--- | :--- |
| `CTL_MOCK_MISSING_PROFILES` | профили через запятую, которых «нет»: их нет в `GET /v4/api/profile`, `statval/last` по ним — 422 `Profile with name … does not exist` |
| `CTL_MOCK_MISSING_ENTITIES` | id сущностей, которых «нет»: их нет в `GET /v4/api/entity`, `statval/last` — 422 `Entity with id … does not exist` |

`GET /v4/api/entity` отдаёт, как боевой CTL, все сущности, а не только дерево профиля:
к `entities.json` добавляются все из `enames.json`, кроме `_Not_found_`. Загрузчик отсекает
висячие ключи из `ctl_events` и перечисляет их заметкой прогона; сенсор событий их больше
не спрашивает.

## Границы

- **Эмулятор — не спецификация.** Он повторяет то, что читает наш код; поля, которых мы
  не касаемся, приезжают из снимка как есть, но у выдуманных объектов (новая загрузка)
  их нет. Расхождение с боем ловится сверкой ответа со снимком, а не эмулятором.
- **Не эмулируются**: права (`/permission*` отвечают «всё можно»), блокировки,
  зависимости воркфлоу (`lastactions`, `stateWithDependencies`), `bulkOperation*`.
  `filtered-compact` есть (поиск для MCP `ctl_workflow`/`ctl_search`), но фильтрует у себя.
- **Лимит частоты — есть.** Все запросы считаются вместе, окно 1 с; сверх `CTL_MOCK_RPS`
  (по умолчанию 10 — как `ctl_rps` в `ctl_config`) эмулятор отвечает 429 и пишет
  предупреждение в свой лог (`CTL_MOCK_RPS_MODE=warn` — только предупреждение). Нужен, чтобы
  видеть суммарную частоту тракта: `rate_limit()` держит порог только внутри процесса. Пики
  за прошлое — по журналу: `select date_trunc('second', ts), count(*) from ctl_mock.api_log …`.
  26.09.2026 пик был 15/с — `ctl_send_html` слал фрагменты отчёта вместе с действиями монитора.
- **Kerberos — настоящий** (MIT KDC в контейнере), но realm свой, `STAND.TEST`, а принципал
  Airflow — `hrplt_etl`, а не боевая учётка.
- **Боевые процедуры GP** — с 26.09.2026 настоящие: движок `pr_swf_start_ctl` и отчёты
  собираются из снимка `GP/` (`testbed/gp_engine/`). Заглушки из `schema.sql` он заменяет.
- **Потоки-отчёты** (`pc1080.mail_*`) в снимке с боя отсутствуют — они в
  `workflows_extra.json`, эмулятор подмешивает их к фикстурам.
- **`ctl_tfs.py` на стенде не собирается**: `ProduceToTopicOperator` в
  `apache-airflow-providers-apache-kafka` 1.15 требует `delivery_callback` строкой-путём,
  а не функцией. На контуре провайдер старше, и там это работает.
