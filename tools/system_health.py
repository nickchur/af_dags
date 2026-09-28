"""### 🩺 DAG: Состояние контура раз в час
*2026-09-28 12:35 MSK · v3.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Почему задачи не идут: S3 логов, пулы, разбор файлов, раны и `scheduled`, плюс сторож
отчётов остальных плагинов. Пишет итог в лог, XCom и заметку; сетка DAG'а — лента здоровья
контура: ❌ — был `error`.

**С 28.09.2026 (v3.0) — два DAG'а в одном файле.** Дешёвое «лежит ли контур» (`components`,
`celery`, `control`, `metabase`, `delivery`) раз в 5 минут проверяет **`tools_system_pulse`**,
здесь — остальное раз в час. `dag_size` переехал в ночной `tools_test_dags` (размер меняется
только с выкладкой), `tables` убран: размеры таблиц метабазы с разницей к прошлому прогону
показывает `report` в `tools_db_cleanup`.

**Таски:** `params` → `collect` (шесть проверок) → `report` (заметка) и `health_warn` /
`health_errors` (итог). Каждая проверка изолирована: своё время, свой таймаут, свой
`try/except` — упавшая даёт свою строку и не мешает остальным.

Проверки обоих DAG'ов, в скобках — чья: пульс или час.

| Проверка | Что смотрит | ⚠️ warn | ❌ error |
|---|---|---|---|
| `components` (пульс) | `get_airflow_health()` — та же функция, что за `/api/v1/health` | triggerer `unhealthy` | метабаза, шедулер или dag-processor `unhealthy` |
| `celery` (пульс) | воркеры через брокер (`broadcast`), длина очередей, счётчики `task_instance` | застрявший `scheduled`, `running` без pid, ждущие при занятых воркерах, занято слотов больше, чем есть | брокер недоступен, ни один воркер не ответил, ждущие при пустых воркерах |
| `control` (пульс) | control-канал celery и брокер: `ping`: ответивших не меньше, чем подов, где сейчас идут задачи (`job`, индекс `job_type_heart`); на узле брокера — задержка команды (`cmd_ms`) и доставки pub/sub самому себе (`pubsub_ms`), `INFO`, права на каналы, подписки-шаблоны | ответили не все поды с задачами; все ответили, но дольше 2 с; команда > 100 мс или pub/sub > 500 мс | не ответил ни один, петля не вернулась, подписок-шаблонов нет |
| `s3_logs` (час) | бакет логов задач: запись, чтение со сверкой, удаление | всё прошло, но дольше `s3_slow_sec` | любая операция упала или прочитано не то |
| `delivery` (пульс) | сколько эта задача ждала воркера и насколько шедулер опоздал с раном | доставка > 60 с, опоздание > 120 с | доставка > 300 с |
| `pools` (час) | `Pool.slots_stats()`: пулы без свободных слотов, в которых ждут задачи | такой пул есть | — |
| `metabase` (пульс) | время `SELECT 1`, соединений против `max_connections` | > 1 с или > 80 % | > 95 % |
| `parsing` (час) | свежесть разбора файлов, DAG'и без сериализации, ошибки импорта, итог ночного `parse_time` | новые ошибки импорта, файл выбился из круга разбора (старше и `min_file_process_interval + dag_file_processor_timeout`, и тройной медианы), разбор стоит целиком, DAG без сериализации, у `parse_time` находки | — |
| `runs` (час) | раны в `queued` дольше 30 мин, раны `queued`/`running` у запаузенных дагов (поимённо, 10 самых старых) | есть раны у запаузенных дагов | — |
| `scheduled` (час) | даги с задачами в `scheduled` дольше 5 мин: сколько, как давно, упёрлись ли в свой `max_active_tasks` (`at_limit`) | застряли даги **не** на своём лимите | — |
| `plugins` (час) | отчёты дагов-плагинов (тег `health`) в `system_health/checks/`: есть ли и не старше ли своего `ttl_sec`. Находки плагинов не повторяет — о них сообщает их `health_errors`; у кого сейчас warn/error, в строке справкой | плагин на паузе, без отчёта или с просроченным отчётом | — |

`collect` находками **не падает**: вердикты уходят в XCom `health`, итог подводят
`health_warn` (есть `warn` — ✅ с ⚠️ в заметке, нет — skip) и `health_errors` (есть `error` —
❌, ран красный, уведомление через `on_callback`; нет — skip). Красный `collect` значит, что
сломалась сама проверка, — `health_errors` назовёт его «не выполнился».

**Плагин здоровья** (тег `health`): `health_errors` пишет отчёт в бакет логов,
`system_health/checks/tools_system_health.json` (`health_tasks` из `plugins/utils.py`), откуда
его читает `get_system_health` MCP — раздел `plugins`. Срок отчёта — 2 ч: пропустил два прогона,
и core скажет «последний отчёт N назад». Раны запаузенных дагов, пулы, `scheduled`, отставание
разбора с etl-core 1.2.1 (HRPDATALAB-15978) проверяются только здесь, core их больше
не считает. Все проверки — SQL к метабазе из таска: в AF3 так нельзя, их придётся перевести на
REST.

**Вывод:** строка на проверку в логе; XCom `return_value` таска `collect` — одна строка на
прогон, `{status, reasons, checks, took_sec}` (до v2.0 таск звался `check`); заметка
`report` — нездоровые проверки по строке, здоровые одной строкой с временами.

| Параметр | Описание |
|---|---|
| ⏰ `schedule` | Расписание (МСК): cron или пресет, пусто — только вручную *(default: `7 * * * *`)* |
| 🐢 `s3_slow_sec` | Сколько секунд на запись, чтение и удаление вместе считать нормой *(default: `5`)* |
| 💾 `save_params` | Сохранить параметры запуска как значения по умолчанию *(default: `False`)* |

**Чего этот DAG не увидит:**

- Когда воркеры не берут задачи, он и сам не запустится. Сигнал тогда — пропуски в сетке
  обоих DAG'ов и растущая `delivery` у пульса.
- Пока на контуре нет etl-core PR #35, висящий S3 может подвесить и лог этой задачи
  (выгрузка из `emit` синхронная). Её снимет `execution_timeout`, и падение по таймауту
  само будет сигналом.
- Длительность разбора по файлам: в метабазе 2.11 её нет. Её меряет ночной `parse_time`
  в `tools_test_dags` (полный `DagBag`, 11 с на альфе и больше 2,5 мин на сигме — поэтому
  не здесь), его последний итог показывается справкой.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        HEALTH_PREFIX, TOOLS_POOL, add_note, ensure_pool, env_stand, health_tasks, on_callback, push_health,
        saved_params, saved_schedule, store_params_task)
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        HEALTH_PREFIX, TOOLS_POOL, add_note, ensure_pool, env_stand, health_tasks, on_callback, push_health,
        saved_params, saved_schedule, store_params_task)

logger = logging.getLogger("airflow.task")

# Москва живёт на постоянном UTC+3 с 2014 года, переходов нет
MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

PARAMS_VAR = "tools_system_health_params"
SAVED = saved_params(PARAMS_VAR)
# На 7-й минуте, а не в :00: в начале часа стартует основная масса расписаний, и проверка
# мерила бы собственную очередь за ними, а не состояние контура
DEFAULT_SCHEDULE = "7 * * * *"
# Срок отчёта плагина: два часовых прогона с запасом
REPORT_TTL_SEC = 2 * 3600 + 600
# Навык агента, на который отчёт плагина отсылает толкование своих проверок
SKILL = "tools-system-health"

RANK = {"healthy": 0, "unknown": 1, "warn": 2, "error": 3}
ICON = {"healthy": "✅", "unknown": "❔", "warn": "⚠️", "error": "❌"}

# ── Пороги ────────────────────────────────────────────────────────────
# Celery — те же, что у карточки Health (etl-core, plugins/celery_health_plugin.py:
# STALE_AFTER_SEC, PROBE_TIMEOUT_SEC), чтобы DAG и карточка называли одно и то же одним словом
STALE_AFTER_SEC = 5 * 60
BROADCAST_TIMEOUT_SEC = 2
# Control-канал: ping ждём дольше broadcast'а — на dev 27.09.2026 ответы опаздывали, и
# двухсекундный опрос не отличал «опоздал» от «потерялся». Под обязан ответить, если на нём
# прямо сейчас идёт задача: LocalTaskJob в running с хартбитом свежее BUSY_HEARTBEAT_SEC
# (хартбит job — каждые 5 с)
PING_TIMEOUT_SEC = 5
BUSY_HEARTBEAT_SEC = 120
# Звенья задержки control-канала: команда брокеру → доставка pub/sub → ответ воркера.
# Пороги — порядок величины, а не замер: redis в одном ЦОД отвечает за единицы мс, и
# сотни мс на команду или полсекунды на доставку уже объясняют опоздавшие ответы
BROKER_CMD_WARN_MS = 100
PUBSUB_WARN_MS = 500
LATENCY_SAMPLES = 3
# Поля INFO узла брокера: хватает, чтобы отличить перегруз и failover от сети
INFO_FIELDS = ("redis_version", "role", "uptime_in_seconds", "connected_clients", "blocked_clients",
               "pubsub_channels", "pubsub_patterns", "instantaneous_ops_per_sec", "used_memory_human",
               "rejected_connections", "evicted_keys")
# S3 — как у промежуточной выгрузки лога (etl-core PR #35, hrp_adapter/logging/handlers.py):
# живой S3 отвечает за доли секунды, сломанный шлюз альфы 11.09.2026 отвечал 504 примерно
# через минуту — одна попытка и короткие таймауты, чтобы проверка не висела вместе с ним
S3_CONNECT_TIMEOUT_SEC = 5
S3_READ_TIMEOUT_SEC = 15
# Доставка: задача в здоровом контуре берётся воркером за секунды. 300 — половина
# task_queued_timeout (600): дальше шедулер сам начинает забирать задачи назад
DELIVERY_WARN_SEC = 60
DELIVERY_ERROR_SEC = 300
SCHED_LAG_WARN_SEC = 120
# Метабаза: SELECT 1 — миллисекунды; секунда значит очередь на соединение или перегруз
DB_PING_WARN_SEC = 1.0
DB_CONN_WARN = 0.80
DB_CONN_ERROR = 0.95
DB_STATEMENT_TIMEOUT_MS = 5000
# Сколько имён показывать в строке проверки: XCom-бэкенд контура не берёт списков длиннее
# 500, а заметка режется по 1000 символов — поимённо нужны только первые
SHOW = 5
# Ран в queued дольше этого: меньше — обычное ожидание max_active_runs, дольше — чаще всего
# даг на паузе
QUEUED_RUN_STALE_SEC = 1800
# Задача в scheduled дольше этого — повод назвать её даг; порог тот же, что у карточки
# воркеров (STALE_AFTER_SEC): меньше — обычное ожидание цикла шедулера
SCHEDULED_STALE_SEC = STALE_AFTER_SEC
# Окно «даг только что был на лимите»: задачи дага, кончившиеся за это время, считаются к
# занятым. Цикл шедулера сигмы — около 7 с, поэтому минуты хватает с запасом
LIMIT_RECENT_SEC = 60
# Сколько дагов и ранов называть поимённо в data
TOP = 10


# ── Общее ─────────────────────────────────────────────────────────────

# Причина исключения наружу — в XCom и заметку — только класс и первая фраза без адресов:
# у kombu в тексте строка подключения с паролем, у SQLAlchemy — хост и порт метабазы.
# Образец — short_reason карточки Health (etl-core, plugins/celery_health_plugin.py);
# полный текст остаётся в логе задачи
_ADDR_RE = re.compile(
    r"""
      \w+://\S+
    | \b\d{1,3}(?:\.\d{1,3}){3}\b(?::\d+)?
    | \b[a-z0-9-]+(?:\.[a-z0-9-]+)+\b(?::\d+)?
    | \b[a-z0-9-]+:\d{2,5}\b
    | \bport\s+\d+
    | \b(?:host|hostname|user|username|password|dbname)=\S+
    """,
    re.VERBOSE,
)


def _short_reason(exc: BaseException) -> str:
    first = str(exc).split("\n")[0]
    head = _ADDR_RE.sub("…", first.split(". ")[0]).strip()
    if len(head) > 80:
        head = head[:77] + "…"
    return f"{type(exc).__name__}: {head}" if head else type(exc).__name__


def _worst(statuses) -> str:
    return max(statuses, key=lambda s: RANK.get(s, 0), default="healthy")


def _age_sec(stamp) -> float | None:
    """Возраст отметки времени (datetime или ISO-строка) в секундах."""
    if stamp is None:
        return None
    if isinstance(stamp, str):
        stamp = datetime.fromisoformat(stamp)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - stamp).total_seconds()


def _names(items, limit: int = SHOW) -> str:
    """Первые limit имён. Списки из SQL выбираются с запасом в одну строку (limit + 1),
    поэтому хвост — «и другие», а не число: сколько их всего, запрос не считал."""
    items = list(items)
    return ", ".join(str(i) for i in items[:limit]) + (" и другие" if len(items) > limit else "")


def _run(name: str, fn, *args) -> dict:
    """Одна проверка: время, статус, итог. Исключение — error с короткой причиной."""
    ts = time.time()
    try:
        result = fn(*args)
    except Exception as exc:
        logger.warning("%s: проверка упала", name, exc_info=True)
        result = {"status": "error", "summary": _short_reason(exc)}
    result["sec"] = round(time.time() - ts, 2)
    logger.info("%s %s (%.2f с): %s", ICON[result["status"]], name, result["sec"], result["summary"])
    return result


def _pg_rows(sql: str, args: dict | None = None, timeout_ms: int = DB_STATEMENT_TIMEOUT_MS) -> list[dict]:
    """SELECT по метабазе с потолком на запрос."""
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        session.execute(text(f"set local statement_timeout = {int(timeout_ms)}"))
        return [dict(r) for r in session.execute(text(sql), args or {}).mappings()]


# ── Проверки ──────────────────────────────────────────────────────────


def check_components() -> dict:
    """То, что штатно отдаёт /api/v1/health: метабаза, шедулер, dag-processor, triggerer.

    Функцию зовём в процессе задачи, а не через REST: Basic-запрос к API стоит 1–3 с
    (FAB проверяет хэш пароля на каждый запрос), а здесь это чтение таблицы job.
    """
    from airflow.api.common.airflow_health import get_airflow_health

    health = get_airflow_health()
    bad = [n for n in ("metadatabase", "scheduler", "dag_processor")
           if (health.get(n) or {}).get("status") == "unhealthy"]
    # Triggerer у нас не поднят, и тогда статус null — это не поломка
    trig = (health.get("triggerer") or {}).get("status") == "unhealthy"
    ages = {}
    for name, key in (("scheduler", "latest_scheduler_heartbeat"),
                      ("dag_processor", "latest_dag_processor_heartbeat")):
        age = _age_sec((health.get(name) or {}).get(key))
        if age is not None:
            ages[name] = round(age)
    beats = ", ".join(f"{n} {a} с" for n, a in ages.items())
    if bad:
        status, summary = "error", f"unhealthy: {', '.join(bad)}" + (f" (хартбит: {beats})" if beats else "")
    elif trig:
        status, summary = "warn", "triggerer unhealthy"
    else:
        status, summary = "healthy", f"хартбит: {beats}" if beats else "компоненты здоровы"
    return {"status": status, "summary": summary, "unhealthy": bad, "heartbeat_age_sec": ages}


# Тот же SQL, что у карточки Health: одним проходом по task_instance, отбор по индексу ti_state.
# Отличие одно — running без pid считается только у задач, стартовавших раньше порога.
# Супервизор ставит running с pid = NULL (_check_and_change_state_before_execution,
# taskinstance.py:2819, Airflow 2.11.2), а pid пишет уже raw-процесс, когда поднимется
# (_run_raw_task, taskinstance.py:256) — это секунды импортов и разбора файла. Без порога
# любая стартующая в момент проверки задача давала бы ложный warn
SQL_TASKS = """
select
    count(*) filter (where state = 'queued')                              as queued,
    count(*) filter (where state = 'queued'
                       and queued_dttm < now() - cast(:age as interval))  as queued_stale,
    count(*) filter (where state = 'running')                             as running,
    count(*) filter (where state = 'scheduled')                           as scheduled,
    count(*) filter (where state = 'scheduled'
                       and updated_at < now() - cast(:age as interval))   as scheduled_stale,
    count(*) filter (where state = 'running' and pid is null
                       and start_date < now() - cast(:age as interval))   as running_no_pid
from task_instance
where state in ('queued', 'running', 'scheduled')
"""
SQL_QUEUES = "select distinct queue from task_instance where state in ('queued', 'running')"


def _queue_lengths(app, names) -> dict:
    """Длина очередей в брокере, по всем приоритетным ключам.

    Длину спрашиваем по короткому имени: kombu сам допишет global_keyprefix к LLEN
    (у нас `{dataplatform}`), а к LRANGE — нет (GlobalKeyPrefixMixin, kombu 5.6.2; подробно —
    tools/queue_analyze.py, _read_queues). Здесь нужна только длина, так что это безопасно.
    """
    out = {}
    with app.connection_for_read() as conn:
        channel = conn.default_channel
        q_for_pri = getattr(channel, "_q_for_pri", None)
        steps = getattr(channel, "priority_steps", [0])
        for name in names:
            keys = {q_for_pri(name, pri) for pri in steps} if q_for_pri else {name}
            out[name] = sum(channel.client.llen(key) or 0 for key in keys)
    return out


def check_celery() -> dict:
    """Воркеры, очереди и задачи — с воркера, где брокер виден всегда.

    Карточка Health опрашивает брокер из вебсервера, и на контуре, где вебсерверу redis
    закрыт (сигма, 12.09.2026), она серая. Отсюда брокер виден: этот процесс сам получил
    задачу через него.
    """
    from airflow.configuration import conf
    from airflow.providers.celery.executors.celery_executor import app

    counters = _pg_rows(SQL_TASKS, {"age": f"{STALE_AFTER_SEC} seconds"})[0]
    counters = {k: int(v or 0) for k, v in counters.items()}
    result = {**counters}
    notes, status = [], "healthy"

    try:
        stats = app.control.broadcast("stats", reply=True, timeout=BROADCAST_TIMEOUT_SEC) or []
        active = app.control.broadcast("active", reply=True, timeout=BROADCAST_TIMEOUT_SEC) or []
    except Exception as exc:
        logger.warning("celery: брокер недоступен", exc_info=True)
        return {**result, "status": "error", "summary": f"брокер недоступен: {_short_reason(exc)}"}

    by_worker = {}
    for chunk in stats:
        by_worker.update(chunk)
    busy_by = {}
    for chunk in active:
        busy_by.update(chunk)
    total = sum(((info or {}).get("pool") or {}).get("max-concurrency") or 0 for info in by_worker.values())
    busy = sum(len(busy_by.get(name) or []) for name in by_worker)
    result.update(workers=len(by_worker), slots_total=total, slots_busy=busy)

    try:
        names = {conf.get("operators", "default_queue", fallback="default")}
        names.update(r["queue"] for r in _pg_rows(SQL_QUEUES) if r["queue"])
        result["queues"] = _queue_lengths(app, sorted(names))
    except Exception as exc:
        logger.warning("celery: очереди не прочитаны", exc_info=True)
        notes.append(f"очереди не прочитаны: {_short_reason(exc)}")

    if not by_worker:
        status = "error"
        notes.insert(0, "ни один воркер не ответил")
    if counters["queued_stale"]:
        waiting = f"ждут дольше {STALE_AFTER_SEC // 60} мин: {counters['queued_stale']}"
        if by_worker and not busy:
            status = "error"
            notes.append(f"{waiting}, воркеры пусты")
        else:
            # Карточка при занятых воркерах молчит; раз в час — стоит сказать: это нехватка
            # ёмкости, и по ленте видно, как часто она случается
            status = _worst([status, "warn"])
            notes.append(f"{waiting}, воркеры заняты — не хватает слотов")
    if total and busy > total:
        status = _worst([status, "warn"])
        notes.append(f"занято слотов больше, чем есть: {busy} из {total}")
    if counters["scheduled_stale"]:
        status = _worst([status, "warn"])
        notes.append(f"застряло в scheduled: {counters['scheduled_stale']}")
    if counters["running_no_pid"]:
        status = _worst([status, "warn"])
        notes.append(f"running без pid: {counters['running_no_pid']}")

    queues = result.get("queues") or {}
    head = (f"воркеров {len(by_worker)}, слоты {busy} из {total}"
            + (", очередь " + ", ".join(f"{n} {v}" for n, v in queues.items()) if queues else ""))
    return {**result, "status": status, "summary": "; ".join([head, *notes])}


# Сколько подов сейчас исполняют задачи: столько узлов обязано ответить на ping. Сверяем
# ЧИСЛА, а не имена: hostname в job у платформы — IP пода (etl-core hostname_callable: по
# нему вебсервер читает живой лог), а узел celery зовётся celery@<имя пода>, и одно из другого
# не получить. Сравнение имён на контурах давало «ни один не ответил» при живых воркерах
# (альфа, 28.09.2026). Из job, а не из task_instance: индекс job_type_heart (job_type,
# latest_heartbeat); task_instance на dev (13 млн строк) по времени не укладывался в 5 с
SQL_BUSY_HOSTS = """
select count(distinct hostname) as busy
  from job
 where job_type = 'LocalTaskJob'
   and state = 'running'
   and latest_heartbeat > now() - cast(:age as interval)
"""


def _plain(value):
    """Ответы redis приходят байтами, в том числе ключи словарей."""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {_plain(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _median_ms(samples: list) -> float | None:
    samples = sorted(x for x in samples if x is not None)
    return round(samples[len(samples) // 2] * 1000, 1) if samples else None


def _broker_pubsub(app) -> dict:
    """Узел брокера, куда ходит сам celery: задержка команд и pub/sub, INFO, права, подписки."""
    out = {}
    # Канал kombu, а не сырой клиент: он уже привязан к узлу кластера по global_keyprefix
    with app.connection_for_read() as conn:
        cli = conn.default_channel.client
        kw = cli.connection_pool.connection_kwargs
        out["node"] = f"{kw.get('host')}:{kw.get('port')}"

        samples = []
        for _ in range(LATENCY_SAMPLES):
            t = time.perf_counter()
            cli.ping()
            samples.append(time.perf_counter() - t)
        out["cmd_ms"] = _median_ms(samples)
        try:
            info = cli.info()
            out["info"] = {k: info[k] for k in INFO_FIELDS if k in info}
        except Exception as exc:
            out["info"] = f"не прочитан: {_short_reason(exc)}"
        # ACL и PUBSUB NUMPAT пользователю брокера бывают закрыты (dev, 28.09.2026:
        # NoPermissionError на оба) — это не поломка pub/sub, петлю проверяем всё равно
        try:
            user = _plain(cli.execute_command("ACL", "WHOAMI"))
            acl = _plain(cli.execute_command("ACL", "GETUSER", user))
            out["acl_channels"] = dict(zip(acl[::2], acl[1::2])).get("channels")
        except Exception as exc:
            out["acl_channels"] = f"не прочитаны: {_short_reason(exc)}"
        try:
            # kombu подписывает воркеры шаблоном (fanout_patterns): PUBSUB CHANNELS их не видит
            out["pattern_subs"] = cli.pubsub_numpat()
        except Exception as exc:
            out["pattern_subs"] = f"не прочитаны: {_short_reason(exc)}"
        ps = cli.pubsub()
        try:
            topic = f"tools_system_health.loopback.{uuid.uuid4().hex[:8]}"
            ps.subscribe(topic)
            ps.get_message(timeout=2)  # подтверждение подписки
            samples = []
            for _ in range(LATENCY_SAMPLES):
                t = time.perf_counter()
                cli.publish(topic, "ping")
                got = ps.get_message(timeout=PING_TIMEOUT_SEC)
                samples.append(time.perf_counter() - t if got is not None else None)
            out["loopback"] = any(x is not None for x in samples)
            out["pubsub_ms"] = _median_ms(samples)
        finally:
            ps.close()
    return out


def check_control() -> dict:
    """Control-канал celery: отвечают ли воркеры на ping через брокер.

    Задачи идут через списки Redis, а ping/inspect — через pub/sub, и второе может молчать
    при живом первом. Так было на dev с 27.09.2026 ~11:10 UTC: воркеры работали, а на
    inspect отвечало 20–40 % опросов (отбивки health_beacon). От этого канала зависят
    liveness-проба воркера (перезапускает под после 20 мин молчания) и число воркеров в
    карточке Health; check_celery видит только итог — меньше воркеров, — а не причину.
    """
    from airflow.providers.celery.executors.celery_executor import app

    busy = int(_pg_rows(SQL_BUSY_HOSTS, {"age": f"{BUSY_HEARTBEAT_SEC} seconds"})[0]["busy"] or 0)
    started = time.time()
    try:
        # limit: ответило столько, сколько подов занято, — не ждём таймаута, и время вызова
        # становится задержкой самого медленного из нужных ответов
        pong = app.control.ping(timeout=PING_TIMEOUT_SEC, limit=busy or None) or []
    except Exception as exc:
        return {"status": "error", "summary": f"брокер недоступен: {_short_reason(exc)}"}
    reply_sec = round(time.time() - started, 1)
    nodes = sorted({name for chunk in pong for name in chunk})
    missing = max(busy - len(nodes), 0)
    result = {"busy_pods": busy, "answered": len(nodes), "nodes": nodes[:TOP], "reply_sec": reply_sec}
    notes, status = [], "healthy"

    try:
        result.update(_broker_pubsub(app))
    except Exception as exc:
        logger.warning("control: pub/sub брокера не проверен", exc_info=True)
        notes.append(f"pub/sub не проверен: {_short_reason(exc)}")

    if result.get("loopback") is False:
        status = "error"
        notes.append("pub/sub брокера не доставляет даже самому себе")
    if busy and not nodes:
        status = "error"
        notes.append(f"ни один воркер не ответил на ping, а задачи идут на {busy} подах")
    elif missing:
        status = _worst([status, "warn"])
        notes.append(f"не ответили {missing} из {busy} подов с задачами")
    elif busy and reply_sec > BROADCAST_TIMEOUT_SEC:
        # Ответы доходят, но позже, чем их ждут карточка Health и отбивки воркеров (2 с) и
        # `celery list-workers` (1 с): там воркеры выглядят пропавшими. dev, 28.09.2026:
        # list-workers пуст, а ping с 5 с получил все 8 ответов
        status = _worst([status, "warn"])
        notes.append(f"ответы идут {reply_sec} с — дольше {BROADCAST_TIMEOUT_SEC} с, которые ждут карточка и отбивки")

    # Где набегает задержка: команда → доставка pub/sub → ответ воркера. Предупреждаем по
    # первому медленному звену — следующие за ним медленны уже из-за него
    cmd_ms, pubsub_ms = result.get("cmd_ms"), result.get("pubsub_ms")
    if cmd_ms is not None and cmd_ms > BROKER_CMD_WARN_MS:
        status = _worst([status, "warn"])
        notes.append(f"брокер медленно отвечает на команды: {cmd_ms} мс — узел или сеть до него")
    elif pubsub_ms is not None and pubsub_ms > PUBSUB_WARN_MS:
        status = _worst([status, "warn"])
        notes.append(f"pub/sub брокера доставляет за {pubsub_ms} мс при командах за {cmd_ms} мс")
    elif nodes and not missing and reply_sec > BROADCAST_TIMEOUT_SEC and pubsub_ms is not None:
        notes.append(f"брокер быстрый (команда {cmd_ms} мс, pub/sub {pubsub_ms} мс) — медлят сами воркеры")
    if busy and result.get("pattern_subs") == 0 and not nodes:
        status = "error"
        notes.append("на узле брокера нет ни одной подписки-шаблона — воркеры не слушают control-канал")

    head = (f"на ping ответили {len(nodes)}, задачи сейчас идут на {busy} подах"
            + (f", за {reply_sec} с" if not missing else "")
            + (f"; брокер {result.get('node')}: команда {cmd_ms} мс, pub/sub {pubsub_ms} мс"
               if cmd_ms is not None else ""))
    return {**result, "status": status, "summary": "; ".join([head, *notes])}


def _log_bucket():
    """Клиент бакета логов с короткими таймаутами и одной попыткой, и имя бакета.

    Подключение и бакет — из [logging] remote_log_conn_id / remote_base_log_folder, а не
    именем: на DEV бакет подменяет airflow_entrypoint. config сливается с botocore-настройками
    подключения, а не заменяет их (см. check_s3_logs).
    """
    from airflow.configuration import conf
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from botocore.config import Config

    conn_id = conf.get("logging", "REMOTE_LOG_CONN_ID")
    bucket = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].partition("/")[0]
    limits = Config(connect_timeout=S3_CONNECT_TIMEOUT_SEC, read_timeout=S3_READ_TIMEOUT_SEC,
                    retries={"total_max_attempts": 1})
    base = S3Hook(aws_conn_id=conn_id).conn_config.botocore_config
    return S3Hook(aws_conn_id=conn_id, config=base.merge(limits) if base else limits).get_conn(), bucket


def check_s3_logs(slow_sec: float, run_id: str) -> dict:
    """Запись, чтение и удаление в бакете логов задач — тем же подключением, что у лога.

    Подключение и бакет — из [logging] remote_log_conn_id / remote_base_log_folder, а не
    именем: на DEV бакет подменяет airflow_entrypoint. verify не переопределяем, в отличие
    от log_cleanup: проверка должна вести себя как обработчик лога (его S3Hook создаётся
    без verify, amazon-провайдер 9.22.0, log/s3_task_handler.py:67), иначе мерила бы не то.

    config не подменяет botocore-настройки подключения, а сливается с ними: явный config у
    хука целиком вытесняет config_kwargs из extra подключения (utils/connection_wrapper.py:241,
    провайдер 9.22.0) — подпись и адресация, без которых шлюз отвергает запросы, молча пропали
    бы. Так же сделано в etl-core PR #35 (partial_hook).

    Ключ — в папке `system_health/` в корне бакета, рядом с отбивками воркеров: там же, где
    всё остальное «не логи» (`dag_snapshots/`, `tfs/`). Объект, оставшийся после сбоя на
    удалении, подберёт log_cleanup — у этой папки свой срок хранения.
    """
    from airflow.configuration import conf

    # Лог в S3 не пишется (локальный компоуз) — проверять нечего, и это не поломка
    if not conf.getboolean("logging", "REMOTE_LOGGING", fallback=False):
        return {"status": "unknown", "summary": "remote_logging выключен — лог в S3 не пишется"}
    key = "system_health/probe.txt"
    # Подключение и клиент — отдельной цифрой: на стенде это 6 с из 6.4 (секрет-бэкенд и
    # сессия boto), сами операции — десятые доли. Медленный секрет-бэкенд не должен
    # выглядеть медленным S3, поэтому в порог s3_slow_sec setup не входит
    ts = time.time()
    client, bucket = _log_bucket()
    setup = round(time.time() - ts, 2)
    extra = {"ServerSideEncryption": "AES256"} if conf.getboolean("logging", "ENCRYPT_S3_LOGS", fallback=False) else {}

    body = f"{run_id} {uuid.uuid4().hex}".encode()
    timings = {}

    def step(op, fn):
        ts = time.time()
        try:
            return fn()
        except Exception as exc:
            timings[op] = round(time.time() - ts, 2)
            logger.warning("s3_logs: %s не прошёл", op, exc_info=True)
            raise RuntimeError(f"{op}: {_short_reason(exc)}") from exc
        finally:
            timings.setdefault(op, round(time.time() - ts, 2))

    try:
        step("put", lambda: client.put_object(Bucket=bucket, Key=key, Body=body, **extra))
        got = step("get", lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read())
        step("delete", lambda: client.delete_object(Bucket=bucket, Key=key))
    except RuntimeError as exc:
        return {"status": "error", "summary": f"s3://{bucket}: {exc}", "timings": timings, "setup_sec": setup}

    total = round(sum(timings.values()), 2)
    ops = ", ".join(f"{op} {sec} с" for op, sec in timings.items())
    if got != body:
        return {"status": "error", "summary": f"s3://{bucket}: прочитано не то, что записано ({ops})",
                "timings": timings, "setup_sec": setup}
    status = "warn" if total > slow_sec else "healthy"
    slow = f" — дольше {slow_sec} с" if status == "warn" else ""
    return {"status": status, "summary": f"s3://{bucket}: {ops}{slow} (подключение {setup} с)",
            "timings": timings, "total_sec": total, "setup_sec": setup}


def check_delivery(ti, dag_run) -> dict:
    """Сколько эта задача ждала воркера и насколько шедулер опоздал с раном.

    Доставка — прямая мера инцидента 11.09.2026: задачи висели в queued, воркеры их не брали.
    Опоздание считаем только у ранов по расписанию: у ручного data_interval_end ни при чём.
    """
    delivery = None
    if ti.queued_dttm and ti.start_date:
        delivery = round((ti.start_date - ti.queued_dttm).total_seconds(), 1)
    lag = None
    if getattr(dag_run, "run_type", None) == "scheduled" and dag_run.start_date and dag_run.data_interval_end:
        lag = round((dag_run.start_date - dag_run.data_interval_end).total_seconds(), 1)

    status, notes = "healthy", []
    if delivery is not None and delivery > DELIVERY_ERROR_SEC:
        status = "error"
    elif delivery is not None and delivery > DELIVERY_WARN_SEC:
        status = "warn"
    if lag is not None and lag > SCHED_LAG_WARN_SEC:
        status = _worst([status, "warn"])
        notes.append("шедулер опоздал")
    parts = [f"ждала воркера {delivery} с" if delivery is not None else "время доставки неизвестно"]
    if lag is not None:
        parts.append(f"ран начат через {lag} с после срока")
    return {"status": status, "summary": ", ".join(parts + notes), "delivery_sec": delivery, "sched_lag_sec": lag}


def check_pools() -> dict:
    """Пулы без свободных слотов, в которых ждут задачи. Только чтение.

    Pool.slots_stats — та же статистика, что на странице Pools. pool_slots из plugins/utils
    не годится: он пишет в базу.
    """
    from airflow.models import Pool
    from airflow.utils.session import create_session

    with create_session() as session:
        stats = Pool.slots_stats(session=session)
    starved = {name: s for name, s in stats.items() if s["open"] <= 0 and s["scheduled"] > 0}
    rows = {name: {"total": s["total"], "open": s["open"], "running": s["running"],
                   "queued": s["queued"], "scheduled": s["scheduled"]}
            for name, s in sorted(starved.items(), key=lambda kv: -kv[1]["scheduled"])[:SHOW]}
    if not starved:
        busy = sum(s["running"] for s in stats.values())
        return {"status": "healthy", "summary": f"пулов {len(stats)}, в работе {busy}, забитых нет", "pools": len(stats)}
    names = _names(f"{n} (слотов {s['total']}, ждут {s['scheduled']})" for n, s in rows.items())
    return {"status": "warn", "summary": f"забиты: {names}", "pools": len(stats), "starved": rows}


def check_metabase() -> dict:
    """Время SELECT 1 и число соединений против max_connections. Глубже — pg_activity."""
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        session.execute(text(f"set local statement_timeout = {DB_STATEMENT_TIMEOUT_MS}"))
        ts = time.time()
        session.execute(text("select 1")).scalar()
        ping = round(time.time() - ts, 3)
        used, limit = session.execute(text(
            "select (select count(*) from pg_stat_activity where datname = current_database()),"
            " current_setting('max_connections')::int"
        )).one()
    share = used / limit if limit else 0
    status = "healthy"
    if share > DB_CONN_ERROR:
        status = "error"
    elif share > DB_CONN_WARN or ping > DB_PING_WARN_SEC:
        status = "warn"
    return {"status": status, "summary": f"SELECT 1 {ping * 1000:.0f} мс, соединений {used} из {limit} ({share:.0%})",
            "ping_sec": ping, "connections": used, "max_connections": limit}


# Свежесть разбора по файлам, DAG'и без сериализации и ошибки импорта — одним заходом по
# метабазе. На стенде 12.09.2026 — 71 мс на 42 файлах. Сами файлы не разбираются: это
# делает ночной parse_time, и он дорог (11 с на альфе, больше 2,5 мин на сигме).
#
# Файл, который dag-processor давно не разбирал, остаётся с активными DAG'ами и старым
# last_parsed_time: Airflow 2.11 деактивирует DAG'и только у файла, который разобран снова
# и этих DAG'ов не дал (dag_processing/manager.py, deactivate_stale_dags). Поэтому старый
# last_parsed_time у активного DAG'а — это именно «файл не разбирается»
SQL_PARSING = """
with f as (
    select fileloc, max(last_parsed_time) as parsed
    from dag where is_active group by fileloc
)
select
    (select count(*) from f)                                                   as files,
    (select extract(epoch from now() - min(parsed)) from f)                    as oldest_sec,
    (select extract(epoch from now() - max(parsed)) from f)                    as newest_sec,
    (select extract(epoch from percentile_cont(0.5) within group (order by now() - parsed)) from f) as median_sec,
    (select count(*) from import_error)                                        as import_errors
"""
SQL_STALE_FILES = """
select fileloc, extract(epoch from now() - max(last_parsed_time)) as age
from dag where is_active group by fileloc
having max(last_parsed_time) < now() - cast(:age as interval)
order by age desc limit :lim
"""
SQL_NOT_SERIALIZED = """
select d.dag_id from dag d
where d.is_active and not exists (select 1 from serialized_dag s where s.dag_id = d.dag_id)
order by d.dag_id limit :lim
"""
# Новая ошибка импорта — со своим id, а не со свежим timestamp: разбирая файл заново,
# Airflow обновляет ту же строку и ставит timestamp = utcnow (airflow 2.11.2,
# dag_processing/processor.py:653), поэтому под фильтром «timestamp новее прошлого рана»
# каждый прогон оказывались ВСЕ ещё не починенные файлы. Починенный файл строку теряет
# (delete там же, 642), новый битый — получает новую строку с новым id из sequence,
# значит id > запомненного и есть «появилась с прошлой проверки».
SQL_NEW_IMPORT_ERRORS = """
select id, filename from import_error where id > :since_id order by id desc limit :lim
"""
SQL_MAX_IMPORT_ERROR = """
select coalesce(max(id), 0) as max_id from import_error
"""


def _last_parse_time() -> dict | None:
    """Итог последнего parse_time из tools_test_dags: XCom return_value и его возраст."""
    from airflow.models.xcom import XCom
    from airflow.utils.session import create_session

    with create_session() as session:
        row = (session.query(XCom)
               .filter(XCom.dag_id == "tools_test_dags", XCom.task_id == "parse_time",
                       XCom.key == "return_value")
               .order_by(XCom.timestamp.desc()).first())
        if row is None:
            return None
        # value уже разобран: BaseXCom десериализует его при загрузке строки
        # (init_on_load → orm_deserialize_value), повторный deserialize_value падает
        value, stamp = row.value, row.timestamp
    if isinstance(value, str):
        value = json.loads(value)
    return {**(value or {}), "age_h": round(_age_sec(stamp) / 3600, 1)}


def _prev_import_error_id(dag_id: str, run_id: str) -> int | None:
    """Наибольший id ошибки импорта, который видел предыдущий прогон этой проверки.

    База сравнения хранится в своём же результате: отдельной переменной для неё заводить
    нечего, а метабаза «когда ошибка появилась» не помнит (см. SQL_NEW_IMPORT_ERRORS).
    Нет предыдущего прогона или в нём нет этого поля — базы нет, и о новых ошибках
    прогон не говорит ничего.
    """
    from airflow.models.xcom import XCom
    from airflow.utils.session import create_session

    with create_session() as session:
        row = (session.query(XCom)
               .filter(XCom.dag_id == dag_id, XCom.task_id.in_(("collect", "check")),
                       XCom.key == "return_value", XCom.run_id != run_id)
               .order_by(XCom.timestamp.desc()).first())
        if row is None:
            return None
        value = row.value
    if isinstance(value, str):
        value = json.loads(value)
    parsing = ((value or {}).get("checks") or {}).get("parsing") or {}
    last = parsing.get("import_error_last_id")
    return int(last) if isinstance(last, (int, float, str)) and str(last).isdigit() else None


def check_parsing(dag_id: str, run_id: str) -> dict:
    """Разбор файлов по метабазе: свежесть, DAG'и без сериализации, ошибки импорта."""
    from airflow.configuration import conf

    # Файл разбирается не реже раза в min_file_process_interval, сам разбор — не дольше
    # dag_file_processor_timeout. Но круг dag-processor'а бывает длиннее их суммы: на стенде
    # 30 + 50 с, а круг — около 160 с (43 файла на два процесса), и порог по одной сумме
    # назвал бы отставшими шесть здоровых файлов. Поэтому отставший — старше и суммы, и
    # тройной медианы: он выбился из круга, по которому идут остальные. На контурах сумма
    # больше: сигма 900 + 270, альфа 300 + 270.
    interval_sum = (conf.getint("scheduler", "min_file_process_interval", fallback=30)
                    + conf.getint("core", "dag_file_processor_timeout", fallback=600))
    head = _pg_rows(SQL_PARSING)[0]
    files, errors = int(head["files"] or 0), int(head["import_errors"] or 0)
    oldest = round(float(head["oldest_sec"] or 0))
    newest = round(float(head["newest_sec"] or 0))
    median = round(float(head["median_sec"] or 0))
    stale_after = max(interval_sum, 3 * median)
    stale = _pg_rows(SQL_STALE_FILES, {"age": f"{stale_after} seconds", "lim": SHOW + 1})
    unserialized = [r["dag_id"] for r in _pg_rows(SQL_NOT_SERIALIZED, {"lim": SHOW + 1})]
    last_id = int(_pg_rows(SQL_MAX_IMPORT_ERROR)[0]["max_id"] or 0)
    prev_id = _prev_import_error_id(dag_id, run_id)
    new_errors = ([r["filename"] for r in _pg_rows(SQL_NEW_IMPORT_ERRORS, {"since_id": prev_id, "lim": SHOW + 1})]
                  if prev_id is not None else [])

    status, notes = "healthy", []
    # Относительный порог не видит остановки целиком: стоит разбор — стареют все файлы
    # вместе с медианой. В работающем круге хоть что-то разобрано недавно
    if files and newest > interval_sum:
        status = "warn"
        notes.append(f"разбор стоит: самый свежий файл разобран {newest} с назад")
    if new_errors:
        status = "warn"
        notes.append(f"новые ошибки импорта: {_names(f.rsplit('/', 1)[-1] for f in new_errors)}")
    if stale:
        status = "warn"
        notes.append(f"не разбираются дольше {stale_after} с: "
                     + _names(f"{r['fileloc'].rsplit('/', 1)[-1]} ({round(float(r['age']))} с)" for r in stale))
    if unserialized:
        status = "warn"
        notes.append(f"без сериализации: {_names(unserialized)}")

    result = {"files": files, "oldest_parse_sec": oldest, "newest_parse_sec": newest, "median_parse_sec": median,
              "import_errors": errors, "new_import_errors": len(new_errors),
              "import_error_last_id": last_id,
              "stale_files": len(stale), "not_serialized": len(unserialized)}
    try:
        last = _last_parse_time()
    except Exception as exc:
        logger.warning("parsing: итог parse_time не прочитан", exc_info=True)
        last, notes = None, [*notes, f"итог parse_time не прочитан: {_short_reason(exc)}"]
    if last:
        result["parse_time"] = {k: last.get(k) for k in ("files", "outliers", "near_timeout", "serialized_gap", "age_h")}
        # Ночной итог действует до следующей ночи; старше суток с запасом — уже не про сейчас
        if last["age_h"] <= 26 and (last.get("near_timeout") or last.get("serialized_gap")):
            status = _worst([status, "warn"])
            notes.append(f"parse_time {last['age_h']} ч назад: у таймаута {last.get('near_timeout') or 0}, "
                         f"не записано {last.get('serialized_gap') or 0}")

    summary = (f"файлов {files}, давний разбор {oldest} с, медиана {median} с, ошибок импорта {errors}"
               + (f"; parse_time {last['age_h']} ч назад: выбросов {last.get('outliers')}" if last else ""))
    return {**result, "status": status, "summary": "; ".join([summary, *notes])}


# ── Перенесено из etl-core (platform_health, до 1.2.1) ────────────────
# AF3: метабаза из таска — в AF3 эти проверки переводятся на REST

# AF2: у рана нет run_after — момент постановки в очередь, а у старых ранов без queued_at —
# execution_date
SQL_RUNS = """
select
    count(*) filter (where dr.state = 'queued'
                       and coalesce(dr.queued_at, dr.execution_date)
                           < now() - cast(:stale as interval))                                   as queued_stale,
    -- trigger_dag паузу не смотрит: ран запаузенного дага висит в queued вечно
    count(*) filter (where d.is_paused)                                                         as paused_active
from dag_run dr
join dag d on d.dag_id = dr.dag_id
where dr.state in ('queued', 'running')
"""

# Сами раны запаузенных дагов, самые старые первыми. Одного счётчика мало: GigaCode 24.09.2026
# на сигме искал пять таких ранов тридцатью вызовами. stuck.queued_runs показывает только
# queued дольше получаса, tools_paused_runs_cleanup — только старше суток, а running не
# показывает никто. Возраст: running — от старта, queued — от постановки в очередь
SQL_PAUSED_RUNS = """
select dr.dag_id, dr.run_id, dr.state,
       cast(extract(epoch from now() - case when dr.state = 'running' and dr.start_date is not null
                                            then dr.start_date
                                            else coalesce(dr.queued_at, dr.execution_date) end)
            as bigint)                                                                   as age_sec
from dag_run dr
join dag d on d.dag_id = dr.dag_id and d.is_paused
where dr.state in ('queued', 'running')
order by age_sec desc, dr.dag_id
limit :top
"""

# Живые scheduled (даг не на паузе, ран в running) старше порога — по дагам. active — задачи
# дага в queued и running: именно их шедулер сравнивает с dag.max_active_tasks, по всем ранам
# сразу. Сигма 23.09.2026: 297 задач в scheduled были задачами raw_to_stable_* при
# max_active_tasks = 4, а в лог шедулера это видно только как «>= max_active_tasks limit».
# Карточка воркеров и публичный /health получают из этого только числа: имена дагов — сюда,
# в MCP, куда без входа не попасть.
#
# active — снимок на один момент, и у дага с короткими задачами он врёт. Сигма 24.09.2026:
# шедулер 3316 раз из 3567 писал «raw_to_stable_lrn_spsimadm has 4/4», задачи шли 7–14 с, а
# между циклами (~7 с) кончившиеся освобождают места. Снимок попадал в эту щель, видел 0–3
# из 4, и health называл даг «не на лимите» с советом смотреть parallelism. Поэтому к active
# прибавляются задачи дага, кончившиеся за последние LIMIT_RECENT_SEC (recent): задача, которая
# ждёт дольше 5 мин, а даг за минуту набирал свой лимит, ждёт именно лимита. recent берётся
# только из ранов в running — по idx_dag_run_running_dags и ti_dag_run, без скана по end_date
SQL_SCHEDULED = """
with waiting as (
    select ti.dag_id, count(*) as scheduled, min(ti.updated_at) as since
    from task_instance ti
    join dag d on d.dag_id = ti.dag_id and not d.is_paused
    join dag_run dr on dr.dag_id = ti.dag_id and dr.run_id = ti.run_id and dr.state = 'running'
    where ti.state = 'scheduled' and ti.updated_at < now() - cast(:stale as interval)
    group by ti.dag_id
)
select w.dag_id, w.scheduled,
       cast(extract(epoch from now() - w.since) as bigint)                   as oldest_sec,
       (select count(*) from task_instance a
         where a.dag_id = w.dag_id and a.state in ('queued', 'running'))      as active,
       (select count(*) from dag_run r
          join task_instance f on f.dag_id = r.dag_id and f.run_id = r.run_id
         where r.dag_id = w.dag_id and r.state = 'running'
           and f.end_date > now() - cast(:recent as interval))               as recent,
       d.max_active_tasks
from waiting w
join dag d on d.dag_id = w.dag_id
order by w.scheduled desc, w.dag_id
"""

def check_runs() -> dict:
    """Раны, которые не начнутся или не кончатся: в queued дольше порога, у запаузенных дагов."""
    counts = _pg_rows(SQL_RUNS, {"stale": f"{QUEUED_RUN_STALE_SEC} seconds"})[0]
    result = {k: int(v or 0) for k, v in counts.items()}
    if not result["paused_active"]:
        return {**result, "status": "healthy",
                "summary": f"в queued дольше {QUEUED_RUN_STALE_SEC // 60} мин: {result['queued_stale']}"}
    runs = _pg_rows(SQL_PAUSED_RUNS, {"top": TOP})
    names = _names(f"{r['dag_id']} ({r['state']}, {round((r['age_sec'] or 0) / 3600, 1)} ч)" for r in runs)
    return {**result, "paused_runs": runs, "status": "warn",
            "summary": f"ранов у запаузенных дагов в queued/running — {result['paused_active']}: {names}; "
                       "пока даг на паузе, они не начнутся и не кончатся"}


def check_scheduled() -> dict:
    """Даги с задачами, давно ждущими в scheduled, и упёрлись ли они в свой max_active_tasks.

    at_limit — running + queued дага вместе с кончившимися за LIMIT_RECENT_SEC (recent) не
    меньше его max_active_tasks: шедулер задачи не отдаёт, потому что так велит даг. Это не
    неисправность, в warn не идёт; warn — только если застряли даги не на лимите.
    """
    rows = _pg_rows(SQL_SCHEDULED, {"stale": f"{SCHEDULED_STALE_SEC} seconds",
                                    "recent": f"{LIMIT_RECENT_SEC} seconds"})
    for row in rows:
        busy = row["active"] + (row.get("recent") or 0)
        row["at_limit"] = row["max_active_tasks"] is not None and busy >= row["max_active_tasks"]
    result = {"stale_after_sec": SCHEDULED_STALE_SEC, "total": sum(r["scheduled"] for r in rows),
              "at_dag_limit": sum(r["scheduled"] for r in rows if r["at_limit"]),
              "dags_total": len(rows), "dags": rows[:TOP]}
    free = [r for r in rows if not r["at_limit"]]
    head = (f"в scheduled дольше {SCHEDULED_STALE_SEC // 60} мин: {result['total']}, "
            f"из них на лимите своего дага {result['at_dag_limit']}")
    if not free:
        return {**result, "status": "healthy", "summary": head}
    return {**result, "status": "warn",
            "summary": f"{head}; не на лимите max_active_tasks — "
                       + _names(f"{r['dag_id']} ({r['scheduled']})" for r in free)
                       + "; проверить пулы и слоты executor'а"}


# Даги-плагины здоровья: тег health, даг жив (разбирается). Отчёт каждого — в бакете логов
SQL_PLUGINS = """
select d.dag_id, d.is_paused
from dag d
join dag_tag t on t.dag_id = d.dag_id and t.name = 'health'
where d.is_active
order by d.dag_id
"""


def check_plugins(own_dag_id: str) -> dict:
    """Отчёты дагов-плагинов на месте и свежие; сами находки плагинов не повторяются.

    О своих ошибках плагин сообщает сам — красным health_errors. Сторож ловит другое: плагин
    замолчал (на паузе, не разбирается, не доходит до health_errors), и его отчёт тихо
    протух. core покажет это только на запрос get_system_health, здесь — в ленте раз в час.
    Свой отчёт не проверяем: его пишет этот же ран, позже этой проверки.
    """
    from airflow.configuration import conf
    from botocore.exceptions import ClientError

    if not conf.getboolean("logging", "REMOTE_LOGGING", fallback=False):
        return {"status": "unknown", "summary": "remote_logging выключен — отчётов плагинов нет"}
    rows = [r for r in _pg_rows(SQL_PLUGINS) if r["dag_id"] != own_dag_id]
    client, bucket = _log_bucket()
    silent, statuses = {}, {}
    for row in rows:
        dag_id = row["dag_id"]
        if row["is_paused"]:
            silent[dag_id] = "на паузе"
            continue
        try:
            body = client.get_object(Bucket=bucket, Key=f"{HEALTH_PREFIX}{dag_id}.json")["Body"].read()
            report = json.loads(body)
        except ClientError as exc:
            missing = exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404")
            silent[dag_id] = "нет отчёта" if missing else f"отчёт не прочитан: {_short_reason(exc)}"
            continue
        except ValueError as exc:
            silent[dag_id] = f"отчёт не разобран: {_short_reason(exc)}"
            continue
        age = _age_sec(report.get("at"))
        if age is None or age > int(report.get("ttl_sec") or 0):
            silent[dag_id] = f"отчёт {round(age / 3600, 1) if age is not None else '?'} ч назад"
        statuses[dag_id] = _worst(c.get("status") for c in (report.get("checks") or {}).values())
    now_bad = {d: st for d, st in statuses.items() if st in ("warn", "error")}
    head = f"плагинов {len(rows)}" + (f"; сейчас {_names(f'{d} {st}' for d, st in now_bad.items())}"
                                      if now_bad else "")
    result = {"plugins": len(rows), "silent": silent, "statuses": statuses}
    if not silent:
        return {**result, "status": "healthy", "summary": f"{head}; отчёты свежие"}
    return {**result, "status": "warn",
            "summary": f"{head}; молчат: " + _names(f"{d} ({why})" for d, why in silent.items())}


# ── Сводка ────────────────────────────────────────────────────────────


def verdict(checks: dict) -> tuple[str, list[str]]:
    """Итоговый статус и причины: худшее из проверок, причины — по нездоровым."""
    status = _worst(c["status"] for c in checks.values())
    reasons = [f"{name}: {c['summary']}" for name, c in checks.items() if c["status"] != "healthy"]
    return status, reasons


def note_text(checks: dict) -> str:
    """Заметка: нездоровые — по строке, здоровые — одной строкой с временами."""
    bad = [f"{ICON[c['status']]} **{name}** — {c['summary']}" for name, c in checks.items() if c["status"] != "healthy"]
    good = [f"{name} {c['sec']} с" for name, c in checks.items() if c["status"] == "healthy"]
    return "\n\n".join(bad + ([f"✅ {', '.join(good)}"] if good else []))


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


# Общее у обоих DAG-ов файла
DEFAULT_ARGS = {
    "owner": "DataLab (CI02420667)",
    "pool": TOOLS_POOL,
    "retries": 0,
    # Как у остальных коротких проверок (tools/readme.md): выше регрессионных прогонов
    # того же пула, ниже агента CTL; absolute — чтобы вес не складывался по цепочке
    "priority_weight": 900,
    "weight_rule": "absolute",
    # Сетевые таймауты проверок в сумме около 90 с: S3 — три операции по 5 + 15 с и чтение
    # отчётов плагинов, метабаза — 5 с на запрос. Пять минут — про зависание
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": on_callback,
}
OWNER_LINKS = {"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"}


def _finish(name: str, checks: dict, started: float, context) -> dict:
    """Итог проверок: строка в лог, вердикты в XCom health; находками не падает."""
    took = round(time.time() - started, 1)
    status, reasons = verdict(checks)
    logger.info("%s %s %s за %.1f с%s", ICON[status], name, env_stand() or "?", took,
                "".join(f"\n  {r}" for r in reasons))
    push_health(checks, context)
    return {"status": status, "reasons": reasons, "checks": checks, "took_sec": took}


@dag(
    doc_md=__doc__,
    owner_links=OWNER_LINKS,
    default_args=DEFAULT_ARGS,
    # Часовой пояс DAG-а берётся из start_date.tzinfo — расписание московское
    start_date=datetime(2026, 9, 14, tzinfo=MSK),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    # Тег tools: служебный DAG — ролевка ограничивает запуск (HRPDATALAB-15421), а
    # get_system_health показывает его в разделе служебных. Тег health — роль: плагин
    # здоровья, get_system_health читает его отчёт (раздел plugins)
    tags=["DataTools", "tools", "health"],
    catchup=False,
    # Смысл DAG'а — непрерывная лента, и он дёшев: включается сам
    is_paused_upon_creation=False,
    max_active_runs=1,
    # При max_active_runs=1 зависший прогон закрывал бы дорогу следующим
    dagrun_timeout=timedelta(minutes=30),
    params={
        "schedule": _param("schedule", DEFAULT_SCHEDULE, type=["string", "null"],
                           description="Расписание: cron или пресет, пусто — только вручную"),
        "s3_slow_sec": _param("s3_slow_sec", 5, type="number", minimum=0.1,
                              description="Запись + чтение + удаление в бакете логов дольше — warn"),
        "save_params": Param(False, type="boolean", description=f"Сохранить параметры запуска в {PARAMS_VAR}"),
    },
)
def tools_system_health():

    @task(task_id="params")
    def save_params(**context):
        """💾 Сохраняет параметры запуска (в т. ч. расписание) как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context)

    # multiple_outputs=False: по аннотации dict Airflow разложил бы ответ на отдельные ключи
    # XCom — четыре лишние строки на прогон, 24 прогона в сутки. NONE_FAILED: params штатно
    # пропускает себя при save_params=False
    @task(task_id="collect", multiple_outputs=False, trigger_rule=TriggerRule.NONE_FAILED)
    def collect(**context) -> dict:
        """Шесть проверок подряд; вердикты — в XCom health, находками таск не падает."""
        dag_run = context["dag_run"]
        started = time.time()
        checks = {
            "s3_logs": _run("s3_logs", check_s3_logs, float(context["params"]["s3_slow_sec"]), dag_run.run_id),
            "pools": _run("pools", check_pools),
            "parsing": _run("parsing", check_parsing, dag_run.dag_id, dag_run.run_id),
            "runs": _run("runs", check_runs),
            "scheduled": _run("scheduled", check_scheduled),
            "plugins": _run("plugins", check_plugins, dag_run.dag_id),
        }
        return _finish("system_health", checks, started, context)

    @task(task_id="report")
    def report(result: dict, **context) -> str:
        """🧾 Заметка: нездоровые проверки по строке, здоровые одной строкой с временами."""
        title = f"{ICON[result['status']]} {result['took_sec']} sec system_health {env_stand() or '?'}"
        add_note(note_text(result["checks"]), context, level="task,DAG", title=title)
        return title

    result = collect()
    save_params() >> result
    report(result)
    result >> health_tasks(ttl_sec=REPORT_TTL_SEC, skill=SKILL)


tools_system_health()


# ── Пульс ─────────────────────────────────────────────────────────────

PULSE_DOC = """### 💓 DAG: Пульс контура раз в 5 минут
*2026-09-28 12:35 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Лежит ли контур прямо сейчас: пять дешёвых проверок, секунды на прогон. Вынесены
28.09.2026 из часового `tools_system_health`, чтобы авария была видна через минуты, а не
через час. Проверки и пороги те же, код общий — `tools/system_health.py`.

**Таски:** `collect` (проверки и заметка) → `health_warn` / `health_errors`. Отдельного
`report` нет: 288 прогонов в сутки, лишний таск на каждом — лишний поход через воркер.
Таска `params` тоже нет — сохранять нечего, расписание в коде.

| Проверка | Что смотрит |
|---|---|
| `components` | `get_airflow_health()`: метабаза, шедулер, dag-processor, triggerer |
| `celery` | воркеры через брокер, длина очередей, счётчики `task_instance` |
| `control` | control-канал celery и узел брокера: `ping`, задержки `cmd_ms` / `pubsub_ms` |
| `metabase` | время `SELECT 1`, соединений против `max_connections` |
| `delivery` | сколько эта задача ждала воркера и насколько шедулер опоздал с раном |

Пороги и толкование — в описании `tools_system_health` и навыке `tools-system-health`.

**Плагин здоровья** (тег `health`): отчёт `system_health/checks/tools_system_pulse.json`, срок
15 минут — три пропущенных прогона, и core скажет «последний отчёт N назад».

**Чего этот DAG не увидит:** когда воркеры не берут задачи, он и сам не запустится. Сигнал
тогда — пропуски в сетке и растущая `delivery` на прогонах вокруг.
"""
# Со 2-й минуты, а не с :00: в начале часа и пятиминуток стартует основная масса расписаний
PULSE_SCHEDULE = "2-59/5 * * * *"
# Три пропущенных прогона
PULSE_TTL_SEC = 15 * 60


@dag(
    doc_md=PULSE_DOC,
    owner_links=OWNER_LINKS,
    # Проверки пульса укладываются в секунды: брокер — два broadcast по 2 с и ping с петлёй
    # pub/sub до 12 с, метабаза — 5 с на запрос
    default_args={**DEFAULT_ARGS, "execution_timeout": timedelta(minutes=2)},
    start_date=datetime(2026, 9, 28, tzinfo=MSK),
    schedule=PULSE_SCHEDULE,
    tags=["DataTools", "tools", "health"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    # Меньше интервала: зависший прогон краснеет до следующего, а не копит очередь за собой
    dagrun_timeout=timedelta(minutes=4),
)
def tools_system_pulse():

    @task(task_id="collect", multiple_outputs=False)
    def collect(**context) -> dict:
        """Пять дешёвых проверок и заметка; вердикты — в XCom health, находками таск не падает."""
        ti, dag_run = context["ti"], context["dag_run"]
        started = time.time()
        checks = {
            "components": _run("components", check_components),
            "celery": _run("celery", check_celery),
            "control": _run("control", check_control),
            "metabase": _run("metabase", check_metabase),
            "delivery": _run("delivery", check_delivery, ti, dag_run),
        }
        result = _finish("system_pulse", checks, started, context)
        title = f"{ICON[result['status']]} {result['took_sec']} sec system_pulse {env_stand() or '?'}"
        add_note(note_text(checks), context, level="task,DAG", title=title)
        return result

    collect() >> health_tasks(ttl_sec=PULSE_TTL_SEC, skill=SKILL)


tools_system_pulse()
