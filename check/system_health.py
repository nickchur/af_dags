"""### 🩺 DAG: Состояние контура раз в час
*2026-09-16 22:32 MSK · v1.3 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Снимает то, что показывает вкладка Health на Cluster Activity, и ещё несколько дешёвых
признаков, пишет итог в лог, XCom и заметку. У карточки нет истории и её видит только тот,
кто её открыл; здесь сетка DAG'а — лента здоровья контура: ❌ — был `error`.

Одна задача `check`, семь проверок. Каждая изолирована: своё время, свой таймаут, свой
`try/except` — упавшая даёт свою строку и не мешает остальным.

| Проверка | Что смотрит | ⚠️ warn | ❌ error |
|---|---|---|---|
| `components` | `get_airflow_health()` — та же функция, что за `/api/v1/health` | triggerer `unhealthy` | метабаза, шедулер или dag-processor `unhealthy` |
| `celery` | воркеры через брокер (`broadcast`), длина очередей, счётчики `task_instance` | застрявший `scheduled`, `running` без pid, ждущие при занятых воркерах, занято слотов больше, чем есть | брокер недоступен, ни один воркер не ответил, ждущие при пустых воркерах |
| `s3_logs` | бакет логов задач: запись, чтение со сверкой, удаление | всё прошло, но дольше `s3_slow_sec` | любая операция упала или прочитано не то |
| `delivery` | сколько эта задача ждала воркера и насколько шедулер опоздал с раном | доставка > 60 с, опоздание > 120 с | доставка > 300 с |
| `pools` | `Pool.slots_stats()`: пулы без свободных слотов, в которых ждут задачи | такой пул есть | — |
| `metabase` | время `SELECT 1`, соединений против `max_connections` | > 1 с или > 80 % | > 95 % |
| `parsing` | свежесть разбора файлов, DAG'и без сериализации, ошибки импорта, итог ночного `parse_time` | новые ошибки импорта, файл выбился из круга разбора (старше и `min_file_process_interval + dag_file_processor_timeout`, и тройной медианы), разбор стоит целиком, DAG без сериализации, у `parse_time` находки | — |

Итог — худший статус. **`error` роняет задачу** (после записи XCom и заметки): в сетке
красный квадрат и уведомление через `on_callback`. `warn` — зелёный прогон с ⚠️ в заметке.

**Вывод:** строка на проверку в логе; XCom `return_value` — одна строка на прогон,
`{status, reasons, checks, took_sec}`; заметка задачи и рана — нездоровые проверки по
строке, здоровые одной строкой с временами.

| Параметр | Описание |
|---|---|
| ⏰ `schedule` | Расписание (МСК): cron или пресет, пусто — только вручную *(default: `7 * * * *`)* |
| 🐢 `s3_slow_sec` | Сколько секунд на запись, чтение и удаление вместе считать нормой *(default: `5`)* |
| 💾 `save_params` | Сохранить параметры запуска как значения по умолчанию *(default: `False`)* |

**Чего этот DAG не увидит:**

- Когда воркеры не берут задачи, он и сам не запустится. Сигнал тогда — пропуски в сетке и
  растущая `delivery` на прогонах вокруг.
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

try:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, env_stand, on_callback, saved_params, store_params,
        valid_schedule)
except ImportError:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, env_stand, on_callback, saved_params, store_params,
        valid_schedule)

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

RANK = {"healthy": 0, "unknown": 1, "warn": 2, "error": 3}
ICON = {"healthy": "✅", "unknown": "❔", "warn": "⚠️", "error": "❌"}

# ── Пороги ────────────────────────────────────────────────────────────
# Celery — те же, что у карточки Health (etl-core, plugins/celery_health_plugin.py:
# STALE_AFTER_SEC, PROBE_TIMEOUT_SEC), чтобы DAG и карточка называли одно и то же одним словом
STALE_AFTER_SEC = 5 * 60
BROADCAST_TIMEOUT_SEC = 2
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


def _pg_rows(sql: str, args: dict | None = None) -> list[dict]:
    """SELECT по метабазе с потолком на запрос."""
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        session.execute(text(f"set local statement_timeout = {DB_STATEMENT_TIMEOUT_MS}"))
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
    check/queue_cleanup.py, _read_queues). Здесь нужна только длина, так что это безопасно.
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
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from botocore.config import Config

    # Лог в S3 не пишется (локальный компоуз) — проверять нечего, и это не поломка
    if not conf.getboolean("logging", "REMOTE_LOGGING", fallback=False):
        return {"status": "unknown", "summary": "remote_logging выключен — лог в S3 не пишется"}
    conn_id = conf.get("logging", "REMOTE_LOG_CONN_ID")
    bucket = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].partition("/")[0]
    key = "system_health/probe.txt"
    limits = Config(connect_timeout=S3_CONNECT_TIMEOUT_SEC, read_timeout=S3_READ_TIMEOUT_SEC,
                    retries={"total_max_attempts": 1})
    # Подключение и клиент — отдельной цифрой: на стенде это 6 с из 6.4 (секрет-бэкенд и
    # сессия boto), сами операции — десятые доли. Медленный секрет-бэкенд не должен
    # выглядеть медленным S3, поэтому в порог s3_slow_sec setup не входит
    ts = time.time()
    base = S3Hook(aws_conn_id=conn_id).conn_config.botocore_config
    client = S3Hook(aws_conn_id=conn_id, config=base.merge(limits) if base else limits).get_conn()
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
               .filter(XCom.dag_id == dag_id, XCom.task_id == "check",
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


def _schedule():
    """Расписание DAG-а: из переменной, если оно осмысленное, иначе из кода."""
    value = SAVED.get("schedule", DEFAULT_SCHEDULE)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {PARAMS_VAR}: расписание '{value}' не разобрано — беру {DEFAULT_SCHEDULE}")
        return DEFAULT_SCHEDULE
    return None if value in (None, "", "None") else str(value).strip()


@dag(
    doc_md=__doc__,
    owner_links={"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"},
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 0,
        # Как у остальных коротких проверок (check/readme.md): выше регрессионных прогонов
        # того же пула, ниже агента CTL; absolute — чтобы вес не складывался по цепочке
        "priority_weight": 900,
        "weight_rule": "absolute",
        # Сетевые таймауты проверок в сумме около 90 с: S3 — три операции по 5 + 15 с,
        # брокер — два broadcast по 2 с, метабаза — 5 с на запрос. Пять минут — про зависание
        "execution_timeout": timedelta(minutes=5),
        "on_failure_callback": on_callback,
    },
    # Часовой пояс DAG-а берётся из start_date.tzinfo — расписание московское
    start_date=datetime(2026, 9, 14, tzinfo=MSK),
    schedule=_schedule(),
    # Тег tools: служебный DAG — ролевка ограничивает запуск (HRPDATALAB-15421), а
    # get_system_health показывает его в разделе служебных
    tags=["DataLab", "tools", "check"],
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

    # multiple_outputs=False: по аннотации dict Airflow разложил бы ответ на отдельные ключи
    # XCom — четыре лишние строки на прогон, 24 прогона в сутки
    @task(task_id="check", multiple_outputs=False)
    def check(**context) -> dict:
        """Семь проверок подряд, итог — в лог, XCom и заметку; error роняет задачу."""
        from airflow.exceptions import AirflowFailException

        state, msg = store_params(PARAMS_VAR, SAVED, context)
        if state == "fail":
            raise AirflowFailException(msg)

        ti, dag_run = context["ti"], context["dag_run"]
        started = time.time()
        checks = {
            "components": _run("components", check_components),
            "celery": _run("celery", check_celery),
            "s3_logs": _run("s3_logs", check_s3_logs, float(context["params"]["s3_slow_sec"]), dag_run.run_id),
            "delivery": _run("delivery", check_delivery, ti, dag_run),
            "pools": _run("pools", check_pools),
            "metabase": _run("metabase", check_metabase),
            "parsing": _run("parsing", check_parsing, dag_run.dag_id, dag_run.run_id),
        }
        took = round(time.time() - started, 1)
        status, reasons = verdict(checks)
        stand = env_stand() or "?"
        logger.info("%s system_health %s за %.1f с%s", ICON[status], stand, took,
                    "".join(f"\n  {r}" for r in reasons))

        result = {"status": status, "reasons": reasons, "checks": checks, "took_sec": took}
        add_note(note_text(checks), context, level="task,DAG",
                 title=f"{ICON[status]} {took} sec system_health {stand}")
        if status == "error":
            # Падающая задача return-значения не оставляет — кладём его сами, иначе лента
            # в XCom теряла бы ровно те прогоны, ради которых она ведётся
            ti.xcom_push(key="return_value", value=result)
            raise AirflowFailException("; ".join(reasons))
        return result

    check()


tools_system_health()
