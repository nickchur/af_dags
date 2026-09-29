"""### 🔌 DAG: Проверка Airflow Connections
*2026-09-29 15:45 MSK · v3.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Автоматизированный аудит и тестирование всех подключений из secret backend.
Ежедневно в 23:15 MSK. Первый таск `collect` снимает список подключений из secret backend
(и обновляет Variable `local_connections` для выпадающих списков kafka-подключений), дальше в
mapped-таск `check` проверяет подключения этого списка — экземпляр на подключение, в сетке
подписан «группа · `conn_id`». Состав проверок всегда свежий: парсинг файла
Variable не читает. До 29.09.2026 список снимал отдельный даг `tools_show_connections`.

| Группа | Условие (conn_id / type) | Описание проверки |
|---|---|---|
| **tfs** | `tfs` в ID **и** тип `aws` | Проверка S3-бакетов через `list_buckets()` |
| **s3** | тип `aws` (без tfs) | Проверка прав доступа к объектному хранилищу |
| **postgres** | тип `postgres` | `SELECT current_user, current_database()` |
| **ctl** | тип `http`, `ctl*` или FQDN | Вызов `GET /v5/api/info` (Kerberos Auth) |
| **clickhouse** | тип `sqlite` / `clickhouse` | Проверка версии через `ClickHouseHook` |
| **kafka** | тип `kafka` | Листинг топиков через `KafkaAdminClientHook` |
| **trino** | тип `trino` | Валидация сессии через `TrinoHook`; нерезолвящийся хост → `☮️` |
| **redis** | тип `redis` | Проверка доступности через `redis.Redis(...).ping()` |
| **other** | прочие | Помечаются символом `☮️` (пропуск) |

**Особенности:**
- **Группы**: набор задан в коде (таблица выше). Флаги `skip_<группа>` (`skip_kafka`, …,
  секция «Пропуск групп»): подключения группы не проверяются (☮️, не ошибка); сохраняются с
  `save_params`.
  Не снялся список (`collect` упал) — ❌ `health_errors`, проверять было нечего.
- **Изоляция**: Сбой одного коннекта не влияет на проверку остальных.
- **Отчетность**: таск `report` формирует Markdown-таблицу со всеми статусами (⭐ — важное) в заметке рана.
- **Важные и вспомогательные**: параметр `critical` — шаблоны `conn_id` (fnmatch), по умолчанию
  `airflowdb`, `ctl`, `s3` и подключение бакета логов; сохраняется с `save_params`. Упавшее
  подключение роняет свой таск, но ран краснеет (❌ `health_errors`, уведомление) только из-за
  важного; вспомогательное — ⚠️ `health_warn`, ран зелёный. Отчёт плагина здоровья пишет
  `health_errors`.
- **Проверка сериализации DAG'ов** вынесена в отдельный DAG `tools_test_dags`
  (`tools/test_dags.py`) — она ждёт парсинга файлов и живёт по своим часам.
"""

from fnmatch import fnmatch
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import Optional

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, ensure_pool, on_callback, health_tasks, push_health, saved_params, saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, ensure_pool, on_callback, health_tasks, push_health, saved_params, saved_schedule, store_params_task,
    )

logger = getLogger("airflow.task")

# Москва живёт на постоянном UTC+3 с 2014 года, переходов на летнее время нет,
# поэтому фиксированное смещение точно описывает пояс и не зависит от tz-базы
MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Расписание — параметр формы: меняется запуском с save_params, без выкладки (как у
# db_cleanup). Пусто — только ручной запуск
PARAMS_VAR = "tools_test_connections_params"
SAVED = saved_params(PARAMS_VAR)
DEFAULT_SCHEDULE = "15 23 * * *"
# Плагин здоровья (тег health): раз в сутки плюс два часа на опоздание прогона
REPORT_TTL_SEC = 26 * 3600
# Навык агента, на который отчёт плагина отсылает толкование упавших подключений
SKILL = "tools-test-connections"
# Жизненно важные подключения — шаблоны conn_id (fnmatch). Упало важное — error, ран красный;
# упало вспомогательное — warn, ран зелёный. Метабаза, CTL, основной S3 и бакет логов (от него
# зависит сам Airflow); шаблоны, которых на контуре нет, ни с чем не совпадают (на сигме CTL нет)
DEFAULT_CRITICAL = sorted({"airflowdb", "ctl", "s3", conf.get("logging", "remote_log_conn_id", fallback="") or "s3"})


# Маппинг Airflow conn_type → тип для chk_any_conn / нативная логика
_CONN_CONFIG: dict[str, str] = {
    "postgres":   "Postgres",
    "aws":         "S3",
    "http":        "KerberosHttp",
    "sqlite":      "ClickHouse",
    "clickhouse":  "ClickHouse",
    "kafka":       "Kafka",
    "trino":       "Trino",
    "redis":       "Redis",
}

def _map_type(conn_type: str) -> Optional[str]:
    """conn_type Airflow → тип проверки; None, если проверять нечем.

    Общая функция, а не два похожих условия в разных местах: группировка в UI и выбор
    проверки обязаны решать одинаково, иначе соединение попадает в группу `ctl`, но
    проверяется как «не реализовано».

    HTTP-подключения из vault приезжают с ХОСТОМ в `conn_type`: `parse_http_connections`
    в `hrp_secret_backend` кладёт туда `host`, а не `"http"` (у `prepare_http_connection`
    в том же файле — честный `"http"`). У conn `ctl` тип получается
    `psi-ctlcom.psidf.sbrf.ru`, и прежнее правило `startswith("ctl")` мимо него
    промахивалось — оно писалось под контур, где хост начинался прямо с `ctl`.
    Точка в типе — надёжный признак FQDN: ни один штатный тип Airflow её не содержит.
    """
    chk = _CONN_CONFIG.get(conn_type)
    if chk is None and ("ctl" in conn_type or "." in conn_type):
        chk = "KerberosHttp"
    return chk


# ---------------------------------------------------------------------------
# Группы проверок: набор постоянный, состав — из collect того же рана
# ---------------------------------------------------------------------------

# Порядок групп в сетке: TFS первым, прочие — последними
GROUPS = ("tfs", "postgres", "s3", "ctl", "clickhouse", "kafka", "trino", "redis", "other")
# Подписи флагов skip_<группа> в форме запуска
_GROUP_TITLE = {
    "tfs": "TFS-соединения", "postgres": "PostgreSQL", "s3": "S3 / Object Storage",
    "ctl": "CTL / HTTP (KerberosHttp)", "clickhouse": "ClickHouse", "kafka": "Kafka",
    "trino": "Trino", "redis": "Redis", "other": "Прочие соединения",
}


def _group(conn_id: str, conn_type: str) -> str:
    """Группа проверки для подключения: tfs, тип хранилища, ctl или other."""
    if "tfs" in conn_id.lower() and conn_type == "aws":
        return "tfs"
    if conn_type == "sqlite":
        return "clickhouse"
    if conn_type == "aws":
        return "s3"
    chk = _map_type(conn_type)
    if chk == "KerberosHttp":
        return "ctl"
    return conn_type if chk is not None and conn_type in GROUPS else "other"


# ---------------------------------------------------------------------------
# Локальная копия plugins.ctl_core.chk_any_conn (Postgres / S3 / KerberosHttp)
# ---------------------------------------------------------------------------

def _chk_any_conn(conn_id: str, conn_type: str, context: dict) -> None:
    """Проверяет доступность соединения (Postgres / S3 / KerberosHttp).

    Самодостаточная копия `plugins.ctl_core.chk_any_conn` — чтобы тест не зависел от
    импорта ctl_core. Логика pool_slots / get_config из оригинала здесь не нужна: тест
    всегда проверяет одно соединение без пулов. При успехе пишет ноту, при ошибке —
    пробрасывает AirflowFailException.
    """
    import time

    from airflow.exceptions import AirflowFailException, AirflowSkipException

    try:
        from plugins.utils import add_note  # type: ignore
    except ImportError:
        from CI06932748.tools.utils import add_note  # type: ignore

    ti = context["ti"]
    try_number = ti.try_number
    sdt = ti.start_date.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    ts = time.time()
    try:
        if conn_type == "Postgres":
            from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
            hook = PostgresHook(postgres_conn_id=conn_id)
            result = hook.get_first("SELECT current_user, current_database(), inet_server_addr()")

        elif conn_type == "S3":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
            from botocore.config import Config  # type: ignore

            extra = S3Hook.get_connection(conn_id).extra_dejson
            verify = extra.get("verify", True)
            if isinstance(verify, str):
                verify = verify.lower() == "true"

            # config_kwargs соединения нельзя терять: явно переданный config подменяет их
            # целиком (connection_wrapper.py: `if not self.botocore_config and config_kwargs`),
            # а секрет-бэкенд кладёт туда signature_version, payload_signing_enabled и
            # request_checksum_calculation — без них шлюз отвергает запросы.
            # merge накладывает таймауты поверх, не затирая остального.
            config = Config(**extra.get("config_kwargs", {})).merge(Config(connect_timeout=15, read_timeout=15))

            hook = S3Hook(aws_conn_id=conn_id, verify=verify, config=config)
            result = hook.get_conn().list_buckets()["Buckets"]

        elif conn_type == "KerberosHttp":
            from hrp_operators.utils.kerberos_http import KerberosHttpHook  # type: ignore

            hook = KerberosHttpHook(method="GET", http_conn_id=conn_id)
            verify = hook.get_connection(conn_id).extra_dejson.get("verify", True)
            if isinstance(verify, str):
                verify = verify.lower() == "true"
            response = hook.run(
                "/v5/api/info",
                headers={"Accept": "application/json"},
                extra_options={"timeout": 15, "verify": verify},
            )
            response.raise_for_status()
            result = response.json()
        else:
            result = None

        logger.info("🔍 %s", result)
        add_note({"try": try_number, "sdt": sdt}, context, title=f"✅ {time.time() - ts:.2f} sec chk_{conn_id}_conn")

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"☮️ {conn_id}: провайдер не установлен — {err}"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        logger.error("❌ %s: %s", conn_id, err, exc_info=True)
        # is not None обязательно: у requests.Response __bool__ == False на 4xx/5xx,
        # то есть проверка на истинность отбросила бы ровно те ответы, ради которых всё и логируется
        response = getattr(err, "response", None)
        if response is not None:
            logger.error("HTTP %s: %s", getattr(response, "status_code", "?"), str(getattr(response, "text", ""))[:500])
        msg = f"❌ {time.time() - ts:.2f} sec chk_{conn_id}_conn ERROR Try {try_number} {sdt}"
        add_note(err, context, level="Task,DAG", title=msg)
        raise AirflowFailException(f"{msg}: {err}") from err


# ---------------------------------------------------------------------------
# Объединенная функция проверки соединений
# ---------------------------------------------------------------------------

def _run_test(conn_id: str, conn_type: str, **context) -> dict:
    """Единая точка входа для проверки всех типов соединений.

    Postgres/S3/HTTP — через chk_any_conn (типы ctl* маппятся на KerberosHttp);
    ClickHouse/Kafka/Trino/Redis — напрямую.
    """
    import time

    from airflow.exceptions import AirflowFailException, AirflowSkipException

    try:
        from plugins.utils import add_note  # type: ignore
    except ImportError:
        from CI06932748.tools.utils import add_note  # type: ignore

    ti = context["ti"]
    ts = time.time()

    # 1. Маппинг типа
    chk_type = _map_type(conn_type)
    if chk_type is not None and conn_type not in _CONN_CONFIG:
        logger.info("Connection '%s' has host-like type '%s', mapping to '%s'", conn_id, conn_type, chk_type)

    if chk_type is None:
        msg = f"☮️ conn_type='{conn_type}' — проверка не реализована"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.info(msg)
        return {"status": "skip", "conn_id": conn_id, "conn_type": conn_type}

    try:
        # 2. Выполнение проверки
        if chk_type in ("Postgres", "S3", "KerberosHttp"):
            _chk_any_conn(conn_id, chk_type, context)
            result = "Success via chk_any_conn"

        elif chk_type == "ClickHouse":
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook  # type: ignore
            hook = ClickHouseHook(clickhouse_conn_id=conn_id)
            result = hook.execute("SELECT version()")

        elif chk_type == "Kafka":
            import confluent_kafka.admin as kafka_admin
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(conn_id)
            conf = conn.extra_dejson.copy()
            if "bootstrap.servers" not in conf:
                conf["bootstrap.servers"] = f"{conn.host}:{conn.port or 9092}"
            conf.setdefault("socket.timeout.ms", 15000)

            client = kafka_admin.AdminClient(conf)
            meta = client.list_topics(timeout=15)
            result = sorted(meta.topics.keys())[:10]

        elif chk_type == "Trino":
            from airflow.providers.trino.hooks.trino import TrinoHook  # type: ignore
            hook = TrinoHook(trino_conn_id=conn_id)
            try:
                result = hook.get_first("SELECT current_user, current_catalog, current_schema")
            except Exception as err:
                # Хост не резолвится: соединение заведено под контур, где Trino нет.
                # Это не деградация связности, а отсутствие сервиса — скип, а не падение.
                # Сообщение приезжает из urllib3 внутрь requests.ConnectionError, поэтому
                # ищем по всей строке, а не сравниваем тип исключения.
                if "Name or service not known" not in str(err):
                    raise
                msg = f"☮️ {conn_id}: хост не резолвится — {err}"
                add_note(msg, context, level="task", title=f"☮️ {conn_id}")
                logger.warning(msg)
                raise AirflowSkipException(msg) from err

        elif chk_type == "Redis":
            import redis
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(conn_id)
            extra = conn.extra_dejson.copy()
            client = redis.Redis(
                host=conn.host,
                port=conn.port or 6379,
                username=conn.login or None,
                password=conn.password or None,
                db=int(extra.get("db", 0)),
                socket_timeout=int(extra.get("socket_timeout", 15)),
            )
            result = f"PONG: {client.ping()}"

        else:
            raise AirflowFailException(f"Logic for {chk_type} not implemented in _run_test")

        # 3. Логирование и выход (для нативных проверок, chk_any_conn сам пишет ноту)
        if chk_type not in ("Postgres", "S3", "KerberosHttp"):
            logger.info("🔍 %s", result)
            msg = f"✅ {time.time() - ts:.2f} sec chk_{conn_id}_conn"
            add_note({"result": str(result)}, context, title=msg)

        return {"status": "ok", "conn_id": conn_id, "conn_type": conn_type}

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"Провайдер не установлен — {err}"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        msg = f"❌ {time.time() - ts:.2f} sec chk_{conn_id}_conn ERROR Try {ti.try_number}"
        add_note(str(err), context, level="task,DAG", title=msg)
        raise  # re-raise оригинальное исключение, а не AirflowFailException — в логе видна причина


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    doc_md=__doc__,
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        # без ретраев: тест снимает срез доступности здесь и сейчас, повтор только
        # растягивает прогон и маскирует моргнувшее соединение
        "retries": 0,
        # 900: выше регрессионных прогонов того же пула (у них приоритета нет),
        # ниже агента CTL (999/1000) — тот двигает боевые загрузки, и обгонять
        # его диагностике незачем.
        #
        # absolute обязателен: правило по умолчанию (downstream) складывает вес
        # вниз по цепочке, и первый таск получил бы 2700 вместо 900 — сравнение с
        # соседями стало бы зависеть от длины цепочки, а не от намерения.
        "priority_weight": 900,
        "weight_rule": "absolute",
        # У каждой проверки соединения свои таймауты в 15 секунд, так что три минуты
        # ловят только зависание — сеть, которая не отвечает и не рвёт соединение.
        "execution_timeout": timedelta(minutes=3),
        "on_failure_callback": on_callback,
    },
    # Часовой пояс DAG'а берётся из start_date.tzinfo (models/dag.py:614-628), поэтому
    # [core] default_timezone = utc не мешает: 23:15 — московские
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    # Ежедневно в 23:15 MSK
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    tags=["DataTools", "tools", "AutoQA", "health"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    # Потолок на прогон, а не только на таск: при max_active_runs=1 зависший
    # прогон закрывает дорогу всем следующим.
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=on_callback,
    params={
        "schedule": Param(
            SAVED.get("schedule", DEFAULT_SCHEDULE), type=["string", "null"], title="Расписание",
            description="cron или пресет (@daily); пусто — только вручную. Применяется со следующего разбора",
        ),
        "critical": Param(
            SAVED.get("critical", DEFAULT_CRITICAL), type="array", items={"type": "string"},
            title="Важные подключения",
            description="Шаблоны conn_id (fnmatch): упало важное — ран красный, остальные — предупреждение",
        ),
        # По флагу на группу: подключения группы не проверяются — ☮️, не ошибка
        **{f"skip_{g}": Param(
            SAVED.get(f"skip_{g}", False), type="boolean", title=f"Пропустить {g}",
            description=_GROUP_TITLE[g], section="Пропуск групп",
        ) for g in GROUPS},
        "save_params": Param(
            False, type="boolean", title="Сохранить параметры",
            description=f"Записать параметры этого запуска в {PARAMS_VAR} как значения по умолчанию",
        ),
    },
)
def tools_test_connections():  # noqa: PLR0915

    # Без потомков: пропуск (save_params=False) ни на что не влияет; report его не считает
    @task(task_id="params")
    def save_params(**context):
        """💾 Сохраняет параметры запуска (в т. ч. расписание) как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context)

    save_params()

    # Список снимается в том же ране: состав проверок всегда свежий, парсинг файла
    # Variable не читает. Один expand на весь список: по XCom-ключу группы Airflow 2
    # раскрывать не умеет, группа — в подписи экземпляра
    @task(task_id="collect", retries=2)
    def collect(**context):
        """🔌 Подключения secret backend: список проверок по группам и Variable local_connections."""
        from collections import defaultdict

        from airflow.configuration import get_custom_secret_backend
        from airflow.exceptions import AirflowFailException
        from airflow.models import Variable

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        backend = get_custom_secret_backend()
        if not hasattr(backend, "_local_connections"):
            raise AirflowFailException(f"{backend} has no attr `_local_connections`")
        conns = backend._local_connections
        logger.info("Loaded %d connections from backend", len(conns))

        # Variable — для выпадающих списков kafka-подключений (test_kafka, ctl_tfs):
        # {conn_type: [{conn_id, host, ...}]}, sqlite показываем как clickhouse
        by_type = defaultdict(list)
        items = []
        for cid, conn in sorted(conns.items()):
            by_type["clickhouse" if conn.conn_type == "sqlite" else conn.conn_type].append({
                "conn_id": cid, "host": conn.host, "port": conn.port, "schema": conn.schema,
                "description": conn.description or "No description",
            })
            items.append({"group": _group(cid, conn.conn_type), "conn_id": cid, "conn_type": conn.conn_type})
        Variable.set("local_connections", dict(by_type), serialize_json=True)

        headers = ["conn_type", "conn_id", "host", "port", "schema", "description"]
        table = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
        for ctype, rows in sorted(by_type.items()):
            table += ["| " + " | ".join(str({"conn_type": ctype, **c}[h]) for h in headers) + " |" for c in rows]
        add_note("\n".join(table), context, level="task", title=f"Connections: {len(conns)} в {len(by_type)} типах")
        return sorted(items, key=lambda i: (GROUPS.index(i["group"]), i["conn_id"]))

    @task(task_id="check", map_index_template="{{ conn_label }}")
    def check(item: dict):
        """Проверка одного подключения; в сетке экземпляр подписан «группа · conn_id»."""
        # item, а не conn: conn — ключ контекста Airflow
        from airflow.operators.python import get_current_context

        from airflow.exceptions import AirflowSkipException

        context = get_current_context()
        context["conn_label"] = f"{item['group']} · {item['conn_id']}"
        if context["params"].get(f"skip_{item['group']}"):
            raise AirflowSkipException(f"☮️ группа {item['group']} пропущена параметром skip_{item['group']}")
        return _run_test(item["conn_id"], item["conn_type"], **context)

    checks = check.expand(item=collect())


    # --- Summary ---
    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):  # noqa: PLR0915
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        dag_run = context["dag_run"]
        # Ключ — (task_id, map_index), а не один task_id: у mapped-таска экземпляров
        # много, task_id у них общий, и словарь по одному task_id оставлял заметку
        # случайного индекса — в строке падения показывалась причина от соседнего,
        # успешного экземпляра
        notes_map: dict[tuple[str, int], str] = {}

        with create_session() as session:
            tis = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                )
                .order_by(TaskInstance.task_id, TaskInstance.map_index)
                .all()
            )
            try:
                from airflow.models.taskinstance import TaskInstanceNote  # noqa: PLC0415
                note_rows = (
                    session.query(TaskInstanceNote)
                    .filter(
                        TaskInstanceNote.dag_id == dag_run.dag_id,
                        TaskInstanceNote.run_id == dag_run.run_id,
                    )
                    .all()
                )
                notes_map = {(n.task_id, n.map_index): (n.content or "") for n in note_rows}
            except Exception as e:
                logger.warning("Could not load task notes: %s", e)

        critical = list(context["params"].get("critical") or [])
        # Номер экземпляра check → подключение: тот же список, по которому шёл expand
        listed = context["ti"].xcom_pull(task_ids="collect") or []
        collect_ok = any(ti.task_id == "collect" and ti.state == "success" for ti in tis)
        failed_vital, failed_aux = [], []
        ok = fail = skip = none_count = 0
        all_rows = []
        durations = []
        icons = []

        for ti in tis:
            # Служебные таски — не проверки соединений
            if ti.task_id in ("report", "params", "collect", "health_warn", "health_errors"):
                continue
            # -1 — не раскрытый check: collect не выполнился или список пуст
            if ti.map_index < 0:
                continue

            state = ti.state
            raw_note = notes_map.get((ti.task_id, ti.map_index), "")
            non_empty_lines = [ln.strip() for ln in raw_note.splitlines() if ln.strip()] if raw_note else []
            first_line = non_empty_lines[2] if len(non_empty_lines) > 2 else ""
            reason = first_line[:120].replace("|", "\\|") or "—"

            if state == "success":
                icon = "✅"
                ok += 1
                reason = "—"
            elif state in ("failed", "upstream_failed"):
                icon = "❌"
                fail += 1
            elif state == "skipped":
                icon = "☮️"
                skip += 1
            elif state is None:
                icon = "🔘"
                none_count += 1
                reason = "Не запущен"
            else:
                icon = "☮️"
                skip += 1

            icons.append(icon)
            if ti.duration:
                durations.append(ti.duration)

            # Выводим только ошибки и скипы; строка — группа и conn_id экземпляра
            item = listed[ti.map_index] if ti.map_index < len(listed) else {}
            conn_id = item.get("conn_id", f"check[{ti.map_index}]")
            name = f"{item.get('group', '?')} · {conn_id}"
            vital = any(fnmatch(conn_id, pat) for pat in critical)
            if icon == "❌":
                (failed_vital if vital else failed_aux).append(conn_id)
            if state != "success":
                all_rows.append(f"| `{name}` | {'⭐' if vital else ''} | {icon} {state or 'not_started'} | {reason} |")

        avg_time = sum(durations) / len(durations) if durations else 0
        graph = "".join(icons)
        counts = f"✅ {ok} / ❌ {fail} / ☮️ {skip}"
        if none_count:
            counts += f" / 🔘 {none_count}"
        headline = f"{graph}\n\n{counts} | 🕒 Avg: {avg_time:.2f}s"

        table = "| Соединение | Важное | Статус | Причина |\n|---|---|---|---|\n" + "\n".join(all_rows)
        add_note(table, context, level="DAG", title=headline)
        logger.info("report: %s", headline)
        # Упавшие подключения таск не роняют: важное — error (ран красный через health_errors),
        # вспомогательное — warn (ран зелёный, ⚠️ в health_warn)
        if not collect_ok:
            # Список не снят — проверять было нечего, это ошибка, а не «всё зелёное»
            failed_vital.insert(0, "collect")
        push_health({
            "connections_critical": {
                "status": "error" if failed_vital else "healthy",
                "summary": (f"упали важные: {', '.join(failed_vital[:10])}" if failed_vital
                            else f"важные в порядке ({', '.join(critical)})"),
                "failed": failed_vital[:20], "critical": critical,
            },
            "connections": {
                "status": "warn" if failed_aux else "healthy",
                "summary": (f"упали вспомогательные: {', '.join(failed_aux[:10])}" if failed_aux else counts),
                "ok": ok, "fail": fail, "skip": skip, "none": none_count, "failed": failed_aux[:20],
            },
        }, context)
        return {"ok": ok, "fail": fail, "skip": skip, "none": none_count, "avg_time": avg_time,
                "failed_critical": failed_vital, "failed_aux": failed_aux}

    report_task = report()
    checks >> report_task
    report_task >> health_tasks(ttl_sec=REPORT_TTL_SEC, skill=SKILL)


tools_test_connections()
