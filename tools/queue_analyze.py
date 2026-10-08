"""### 🔬 Разбор очереди: почему задачи ждут, и мусор в брокере
*2026-10-01 22:37 MSK · v3.24 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в час (в :10) разбирает, почему задачи ждут: лимиты дагов, пулы, приоритет, окно
шедулера, ёмкость исполнителя, мусор в брокере и раздутый сет привязок ответных очередей.
Выводы словами — в заметке `report`, разбор целиком — `queue_cleanup/<дата>/` в бакете логов.
Плагин здоровья, вердикт не выше ⚠️: очередь — не авария.

**Таски:** `params` → `broker`, `scheduler`, `capacity`, `pidbox` → `purge` (по галке, после
`broker`), `purge_pidbox` (по галке, после `pidbox`) → `report` → `health_warn` / `health_errors`.

Подробно: [tools/readme.md — queue_analyze](../../_plugin_dag_docs/?doc=tools/readme.md#queue_analyzepyqueue_analyzepy)
"""

# tuple | None в сигнатуре: на 3.9 аннотация вычисляется при определении функции и
# падает без этого импорта, а контуры на разных версиях питона.
from __future__ import annotations

# Только то, что нужно на разборе файла: декораторы, Param/TriggerRule для сигнатуры,
# datetime для start_date. Всё runtime-only — брокер, S3, метабаза — внутри тасков.
from datetime import datetime, timedelta, timezone
import base64
import json
import logging
import time
import uuid

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, Feed, add_note, ensure_pool, health_tasks, on_callback, push_health, saved_params, saved_schedule,
        env_platform, env_stand, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, Feed, add_note, ensure_pool, health_tasks, on_callback, push_health, saved_params, saved_schedule,
        env_platform, env_stand, store_params_task,
    )

logger = logging.getLogger("airflow.task")

# Пул заводим при разборе файла: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Бакет и коннект — те же, что у логов задач, папка своя: с логами в выдаче не мешается,
# старое убирает tools_log_cleanup общим сроком бакета.
# verify=False у S3Hook ниже — как во всех дагах этого репозитория: наш S3-шлюз ходит
# по внутреннему сертификату, которого нет в бандле CA у образа. Правильное решение —
# CA в подключении; пока его нет, оставляем как есть, но помним, что это не «на всякий
# случай», а осознанный компромисс.
AWS_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
BUCKET_NAME = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].split("/")[0]
# Папка прежнего tools_queue_cleanup: старые дампы и их чистка не теряются
PREFIX = "queue_cleanup/"

PARAMS_VAR = "tools_queue_analyze_params"
#: Переменная прежнего tools_queue_cleanup: её пороги — значения по умолчанию, пока новая
#: не сохранена. Переносится первым же запуском с save_params
OLD_VAR = "tools_queue_cleanup_cfg"

# Значения по умолчанию. Всё, кроме purge: удаление не должно становиться настройкой,
# живущей между запусками, — галку ставят руками на конкретный ран.
DEFAULTS = {
    "queues": "",
    # Ниже этой доли не удаляем: значит картина не та, для которой инструмент сделан.
    # На альфе доля мусора была ~0.97, так что порог в половину очереди не мешает делу,
    # но останавливает запуск по здоровой очереди, где размечено три сообщения из девятисот.
    "min_junk_share": 0.5,
    # Верхний предел на одно удаление: страховка от ошибки в разметке, а не от объёма.
    "max_delete": 5000,
    # Как STALE_AFTER_SEC карточки Health (etl-core celery_health_plugin): младше — обычное
    # ожидание цикла шедулера
    "stale_min": 5,
    # Каждый час в :10 (до 01.10.2026 — раз в сутки «10 9 * * *» MSK): прогон короче минуты,
    # пустая очередь разбирается даром, а при нагрузке вывод нужен сразу, не наутро
    "schedule": "10 * * * *",
    # Привязок ответных очередей больше этого — вывод 📮 и ⚠️ плагина. Живых опрашивающих
    # (маяки, liveness-пробы, пульс) — единицы на под, тысяча уже значит утечку
    "max_reply_bindings": 1000,
}
ONE_SHOT = ("purge", "purge_pidbox")
MSK = timezone(timedelta(hours=3))
# Плагин здоровья (тег health): сутки плюс два часа — переживает и сохранённое суточное расписание
REPORT_TTL_SEC = 26 * 3600
# Навык агента, на который отчёт плагина отсылает толкование выводов
SKILL = "tools-queue-analyze"
# Выводы, которые дают ⚠️ (по значку в начале строки conclusions): мусор в брокере, голодание
# по приоритету, раны без старта. Лимиты дагов и пулы — ограничения в коде дагов, справка
WARN_MARKS = ("🗑️", "⚖️", "⏱️", "📮")
# Сет привязок ответных очередей control-канала celery (kombu: keyprefix_queue % exchange).
# Отвечая на ping/inspect, воркер читает его целиком (SMEMBERS в get_table) в главном цикле:
# на раздутом сете он минутами не берёт задачи и молчит на ping — сигма dev 30.09.2026,
# стек kill -USR1. Обычно kombu убирает свою запись сам после сбора ответов (стенд: сет пуст)
REPLY_BINDINGS_KEY = "_kombu.binding.reply.celery.pidbox"
# Сколько записей сета показать в заметке
REPLY_SAMPLE = 5
# Добор сета через SREM, если DEL его не удалил: записей за шаг и предел по времени
PURGE_BATCH = 1000
PURGE_SREM_SEC = 300

SAVED = saved_params(PARAMS_VAR)
_cfg = {**DEFAULTS, **{k: v for k, v in saved_params(OLD_VAR).items() if k in DEFAULTS}, **SAVED}

# Состояния, из которых задача уже не вернётся. Всё остальное, включая пустое состояние,
# считаем живым: направление ошибки выбрано в пользу сохранения сообщения.
#
# Список задан явно, а не через State.finished: finished включает removed и не включает
# None, и его состав между версиями Airflow менялся. Здесь он решает, что удалять, —
# такой список должен быть виден глазами, а не подразумеваться.
TERMINAL_STATES = frozenset({"success", "failed", "skipped", "upstream_failed", "removed"})

# Партия для IN по кортежам: планировщик разбирает список из тысяч элементов заметно дольше.
STATE_BATCH = 500
# Предел на чтение: тела читаются в память целиком, иначе разметить их нечем.
# Гигабайтная очередь — уже не случай этого инструмента, и упасть на входе честнее, чем
# по OOM в середине разбора. Проверяется по LLEN, до LRANGE, то есть даром, — и по
# сумме тоже: в памяти лежат тела всех очередей сразу, поэтому пять очередей по сорок
# тысяч ничем не лучше одной на двести.
READ_LIMIT = 50_000
# Потолок выборки scheduled: на сигме 23.09 их было 297; десятки тысяч — уже авария,
# и для выводов хватит первой партии (в отчёте помечено)
SCHEDULED_LIMIT = 20_000



# Очереди, к которым задачи привязаны в метабазе. Смотрим не только queued: сообщение
# переживает свою задачу, и очередь давно завершившейся задачи всё ещё нужно обойти.
SQL_QUEUES = """
SELECT DISTINCT queue FROM main.task_instance WHERE queue IS NOT NULL
"""

# Состояние задач, названных в сообщениях. Отбор по первичному ключу
# (dag_id, task_id, run_id, map_index) — то есть по индексу, а не сканом: спрашиваем ровно
# про те задачи, что нашлись в очереди.
#
# Отбор по external_executor_id пришлось бы делать сканом (индекса по нему нет, замер на
# соседней карточке Health — 330 мс на боевой), и живости он всё равно не показывает:
# у завершившейся задачи там стоит идентификатор того самого сообщения, которое и нужно
# удалить. Поэтому он здесь только для сверки в отчёте.
SQL_TARGETS = """
SELECT dag_id, task_id, run_id, map_index, state, external_executor_id
  FROM main.task_instance
 WHERE (dag_id, task_id, run_id, map_index) IN :keys
"""


def _decode(raw) -> dict:
    """Разбирает сырое сообщение очереди в словарь. Ключи те, что нужны разметке."""
    text = raw.decode() if isinstance(raw, bytes) else raw
    msg = json.loads(text)
    headers = msg.get("headers") or {}
    props = msg.get("properties") or {}
    body = msg.get("body")
    if props.get("body_encoding") == "base64":
        body = base64.b64decode(body)
    # Протокол celery v2: тело — [args, kwargs, embed]. Команда Airflow лежит первым
    # позиционным аргументом execute_command. Разбираем тело, а не headers.argsrepr:
    # repr придётся парсить как питон, а тело — обычный JSON.
    # Чужое тело (не список, пустой список, args не список) — команды нет, id и task остаются.
    # Первый элемент — через next(iter()), без индекса: пустой список даёт None, а не IndexError
    payload = json.loads(body) if body else None
    args = next(iter(payload), None) if isinstance(payload, list) else None
    command = next(iter(args), None) if isinstance(args, list) else None
    return {
        "id": headers.get("id"),
        "task": headers.get("task"),
        "command": command if isinstance(command, list) else None,
    }


def _target(command) -> tuple | None:
    """Задача, названную которой несёт команда: (dag_id, task_id, run_id, map_index).

    None — цель не читается: не `airflow tasks run`, укороченная команда, чужая задача
    celery. Такое сообщение остаётся в очереди.
    """
    if not command or command[:3] != ["airflow", "tasks", "run"] or len(command) < 6:
        return None
    map_index = -1
    if "--map-index" in command:
        try:
            map_index = int(command[command.index("--map-index") + 1])
        except (IndexError, ValueError):
            return None
    return command[3], command[4], command[5], map_index


def _states(keys: list) -> dict:
    """Состояние и идентификатор celery по ключам задач: {ключ: (state, executor_id)}.

    Порциями: список ключей приходит из очереди и на инциденте бывает в тысячи строк,
    а IN по кортежам с таким числом элементов планировщик разбирает заметно дольше.
    """
    from airflow import settings
    from sqlalchemy import bindparam, text

    out = {}
    if not keys:
        return out
    sql = text(SQL_TARGETS).bindparams(bindparam("keys", expanding=True))
    with settings.engine.connect() as conn:
        for i in range(0, len(keys), STATE_BATCH):
            rows = conn.execute(sql, {"keys": keys[i:i + STATE_BATCH]})
            for dag_id, task_id, run_id, map_index, state, eid in rows:
                out[(dag_id, task_id, run_id, map_index)] = (state, eid)
    return out


def _read_queues(names: list) -> tuple:
    """Читает все приоритетные ключи очередей. Возвращает (сообщения, префикс, ключи).

    Сообщение — dict с сырым телом и ключом, по которому его удалять и возвращать.
    """
    from airflow.providers.celery.executors.celery_executor import app

    msgs, keys = [], []
    with app.connection_for_read() as connection:
        channel = connection.default_channel
        prefix = getattr(channel, "global_keyprefix", "") or ""
        for name in names:
            for pri in getattr(channel, "priority_steps", [0]):
                base = channel._q_for_pri(name, pri)
                # kombu дописывает global_keyprefix НЕ ко всем командам redis: LLEN
                # получает, LRANGE и LREM — нет (GlobalKeyPrefixMixin, kombu 5.6.2).
                # Поэтому длину спрашиваем по короткому имени — префикс допишет клиент,
                # а содержимое и удаление идут по полному, которое собираем сами.
                full = prefix + base
                # Обе команды одним MULTI/EXEC: на живой очереди между отдельными
                # llen и lrange успевает пройти BRPOP воркера, длины расходятся на
                # единицу и сверка ниже валит весь прогон на ровном месте.
                # pipeline() у префиксного клиента — PrefixedRedisPipeline, правила
                # префикса те же (kombu 5.6.2), поэтому имена ключей не меняем.
                # Читаем не больше предела: иначе гигантская очередь приедет в память
                # раньше, чем сработает проверка READ_LIMIT.
                with channel.client.pipeline() as pipe:
                    pipe.llen(base)
                    pipe.lrange(full, 0, READ_LIMIT)
                    length, body = pipe.execute()
                if length > READ_LIMIT or len(msgs) + length > READ_LIMIT:
                    raise RuntimeError(
                        f"ключ {base!r}: {length} сообщений, уже прочитано {len(msgs)}, "
                        f"предел чтения — {READ_LIMIT}. Столько этот инструмент в память не "
                        f"берёт: разбирать такую очередь нужно иначе — пересборкой ключа, а не "
                        f"удалением по одному сообщению."
                    )
                # Не «на всякий случай»: на контуре с префиксом это дало «в очереди 0»
                # при llen = 578 — удаление молча било бы мимо очереди. Стенд без
                # префикса такую ошибку не ловит, поэтому сверка живёт в коде.
                if length != len(body):
                    raise RuntimeError(
                        f"ключ {base!r}: llen={length}, а прочитано {len(body)}. "
                        f"Ключи разошлись, разметка недостоверна — ничего не удаляем."
                    )
                keys.append({"key": full, "length": length})
                for raw in body:
                    msgs.append({"key": full, "queue": name, "priority": pri, "raw": raw})
    return msgs, prefix, keys


def classify(msgs: list, states: dict) -> tuple:
    """⤴️ Размечает сообщения. Возвращает `(status, payload)`, решает вызывающий таск.

    Живым сообщение остаётся, если названная им задача есть в метабазе и не в терминальном
    состоянии, либо если цель вообще не прочиталась. Мусор — всё остальное.
    """
    live, junk, reasons, unparsed, eid_mismatch = [], [], {}, 0, 0
    for m in msgs:
        info = m["info"]
        key = _target(info.get("command"))
        if key is None:
            unparsed += 1
            live.append(m)
            reasons["цель не прочиталась"] = reasons.get("цель не прочиталась", 0) + 1
            continue
        state, eid = states.get(key, (None, None))
        if key not in states:
            junk.append(m)
            reasons["задачи нет в метабазе"] = reasons.get("задачи нет в метабазе", 0) + 1
        elif state in TERMINAL_STATES:
            junk.append(m)
            reasons[f"задача уже {state}"] = reasons.get(f"задача уже {state}", 0) + 1
        else:
            live.append(m)
            reasons[f"задача {state or 'без состояния'}"] = reasons.get(f"задача {state or 'без состояния'}", 0) + 1
        # Сверка, а не признак: расхождение показывает, насколько разметка по состоянию
        # расходится с тем, что метабаза помнит о последнем сообщении задачи.
        if eid and eid != info.get("id"):
            eid_mismatch += 1

    if not msgs:
        return "skip", {"note": "очередь пуста — размечать нечего"}
    return "ok", {
        "live": live,
        "junk": junk,
        "reasons": reasons,
        "unparsed": unparsed,
        "eid_mismatch": eid_mismatch,
    }


def _median(values: list):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def scheduler_causes(waiting: list, active: dict, pools: dict, dispatched: list, now, stale_min: int,
                     window: int = None) -> dict:
    """Почему ждут задачи в ``scheduled`` — без Airflow, чтобы проверялось тестом.

    Args:
        waiting: ``[{dag_id, pool, priority_weight, updated_at, is_paused, run_state, max_active_tasks,
            logical_date}]``.
        active: ``{dag_id: queued+running}``.
        pools: ``{pool: {'slots': n, 'used': n}}``.
        dispatched: веса задач, стартовавших за последний час.
        now: Текущее время (aware).
        stale_min: Порог «давно ждёт», мин.
        window: Сколько задач шедулер берёт из scheduled за цикл (``max_tis_per_query``, не больше
            ``parallelism``); None — окно не оцениваем.

    Returns:
        ``{'live', 'parked', 'stale', 'by_cause': {причина: n}, 'dags': [...], 'median_dispatched',
        'window'}``. Причина у давно ждущей живой задачи одна, по порядку проверки: лимит дага →
        пул → окно → приоритет → прочее. Лимит дага первым: при нём шедулер задачу не возьмёт при
        любом весе и любом пуле.

        «Окно»: шедулер AF 2 берёт из scheduled первые ``window`` задач по весу, затем по дате рана,
        и кончает цикл, как только среди них нашлось что запустить (``is_done`` в
        ``_executable_task_instances_to_queued``). Если голову занимают задачи дагов на своём
        лимите, задача за окном не рассматривается вовсе — при свободных воркерах и том же весе,
        что у ушедших в работу (медиана тут не поможет). Сигма dev 01.10.2026: окно 63, из них ~53
        на лимите дагов, 306 задач ждали без видимой причины, ручной tools_db_cleanup — 43 мин.
    """
    edge = now - timedelta(minutes=stale_min)
    median = _median(dispatched)

    def at_limit(w):
        limit = w.get("max_active_tasks")
        return limit is not None and active.get(w["dag_id"], 0) >= limit

    # Порядок выборки шедулера: вес по убыванию, затем дата рана
    floor = datetime.min.replace(tzinfo=timezone.utc)
    order = sorted((w for w in waiting if not w.get("is_paused") and w.get("run_state") == "running"),
                   key=lambda w: (-(w.get("priority_weight") or 0), w.get("logical_date") or floor))
    rank = {id(w): i for i, w in enumerate(order)}
    head = order[:window] if window else []
    blocked = {}
    for w in head:
        if at_limit(w):
            blocked[w["dag_id"]] = blocked.get(w["dag_id"], 0) + 1
    dags, by_cause = {}, {}
    live = parked = stale = 0
    for w in waiting:
        if w.get("is_paused") or w.get("run_state") != "running":
            parked += 1
            continue
        live += 1
        d = dags.setdefault(w["dag_id"], {
            "dag_id": w["dag_id"], "waiting": 0, "stale": 0, "oldest_min": 0,
            "active": active.get(w["dag_id"], 0), "max_active_tasks": w.get("max_active_tasks"),
            "weights": set(), "causes": {},
        })
        d["waiting"] += 1
        d["weights"].add(w.get("priority_weight"))
        since = w.get("updated_at")
        if not since or since >= edge:
            continue
        stale += 1
        d["stale"] += 1
        d["oldest_min"] = max(d["oldest_min"], int((now - since).total_seconds() // 60))
        pool = pools.get(w.get("pool")) or {}
        if d["max_active_tasks"] is not None and d["active"] >= d["max_active_tasks"]:
            cause = "лимит дага"
        elif pool and pool["slots"] >= 0 and pool["used"] >= pool["slots"]:
            cause = f"пул {w.get('pool')}"
        elif window and rank[id(w)] >= window and blocked:
            cause = "окно"
        elif median is not None and (w.get("priority_weight") or 0) < median:
            cause = "приоритет"
        else:
            cause = "прочее"
        d["causes"][cause] = d["causes"].get(cause, 0) + 1
        by_cause[cause] = by_cause.get(cause, 0) + 1
    out = []
    for d in dags.values():
        weights = sorted(x for x in d.pop("weights") if x is not None)
        d["weight"] = f"{weights[0]}–{weights[-1]}" if len(weights) > 1 else (weights[0] if weights else None)
        out.append(d)
    out.sort(key=lambda d: (-d["stale"], -d["waiting"]))
    return {"live": live, "parked": parked, "stale": stale, "by_cause": by_cause, "dags": out,
            "median_dispatched": median, "dispatched_1h": len(dispatched),
            "window": {"size": window, "blocked": sum(blocked.values()),
                       "blocked_dags": sorted(blocked.items(), key=lambda x: -x[1]),
                       "behind": max(len(order) - window, 0)} if window else None}


def _scheduler_window() -> int:
    """Сколько задач шедулер берёт из scheduled за цикл: ``max_tis_per_query`` (0 — без предела),
    не больше ``parallelism``. Верхняя оценка: занятые слоты executor'а окно сужают ещё.
    ``parallelism`` — из конфига воркера, на котором идёт таск; у шедулера он обычно тот же.
    """
    parallelism = conf.getint("core", "parallelism")
    per_query = conf.getint("scheduler", "max_tis_per_query", fallback=16)
    return parallelism if not per_query else min(per_query, parallelism)


def _count(res) -> int:
    """Число из ответа Redis. Альфа dev 30.09: SREM ответил списком, а не числом, — похоже на
    прокси перед брокером, который шлёт команду на несколько узлов и возвращает их ответы."""
    if isinstance(res, (list, tuple)):
        return sum(_count(r) for r in res)
    return res if isinstance(res, int) else 0


def _reply(res):
    """Ответ как есть, если он не число: по нему видно, что вернул сервер."""
    return res if isinstance(res, int) else repr(res)[:200]


def _probe(client, full: str) -> dict:
    """Какие удаления брокер выполняет на самом деле: на маленьком временном сете и на большом.

    Альфа и сигма dev 30.09: DEL сета ответил 1, SREM — `[]`, а сет не изменился; на стенде те же
    команды работают. kombu убирает привязку ответной очереди тем же SREM — если он на контуре
    не работает, это и есть утечка. Временный ключ с тем же хэштегом (тот же узел), живёт минуту.
    """
    out = {}
    tmp = full.split("}", 1)[0] + "}_tools_probe:" + uuid.uuid4().hex[:8] if full.startswith("{") \
        else "_tools_probe:" + uuid.uuid4().hex[:8]
    try:
        out["tmp_sadd"] = _reply(client.sadd(tmp, "a", "b", "c"))
        client.expire(tmp, 60)
        out["tmp_srem"] = _reply(client.srem(tmp, "a"))
        out["tmp_scard"] = _reply(client.scard(tmp))
        # SMOVE во временный ключ + UNLINK — обход SREM, которым etl-core снимает привязку
        moved = tmp + ":m"
        out["smove"] = _reply(client.smove(tmp, moved, "b"))
        client.expire(moved, 60)
        out["smove_left"] = _reply(client.sismember(tmp, "b"))
        out["smove_dst"] = _reply(client.scard(moved))
        out["tmp_del"] = _reply(client.delete(tmp))
        out["tmp_exists"] = _reply(client.exists(tmp))
        out["tmp_ttl"] = _reply(client.ttl(tmp))
        # Команды, которыми kombu подтверждает задачу (HDEL unacked, ZREM unacked_index) и снимает
        # сообщение (LREM): не выполняются — подтверждённое вернётся в очередь по visibility_timeout
        h, z, lst = tmp + ":h", tmp + ":z", tmp + ":l"
        client.hset(h, "k", "v")
        client.zadd(z, {"m": 1})
        client.rpush(lst, "a")
        for key in (h, z, lst):
            client.expire(key, 60)
        out["hdel"] = _reply(client.hdel(h, "k"))
        out["hlen"] = _reply(client.hlen(h))
        out["zrem"] = _reply(client.zrem(z, "m"))
        out["zcard"] = _reply(client.zcard(z))
        out["lrem"] = _reply(client.lrem(lst, 0, "a"))
        out["llen"] = _reply(client.llen(lst))
        out["unlink"] = _reply(client.unlink(tmp))
        out["unlink_exists"] = _reply(client.exists(tmp))
        member = client.srandmember(full)
        if member is not None:
            out["big_srem1"] = _reply(client.srem(full, member))
            out["big_ismember"] = _reply(client.sismember(full, member))
            before = client.scard(full)
            out["big_spop"] = repr(client.spop(full))[:120]
            out["big_spop_delta"] = before - client.scard(full)
    except Exception as exc:  # noqa: BLE001 — проба, не повод падать
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def reply_bindings(purge: bool = False) -> dict:
    """Размер сета привязок ответных очередей; purge — удалить сет целиком.

    Удаление безопасно для задач: в сете только адреса ответов на служебные команды. Ответная
    очередь kombu — auto_delete, её объявление не кэшируется, и живой опрашивающий вернёт
    свою запись при следующем broadcast, потеряв не больше одного ответа.
    """
    from airflow.providers.celery.executors.celery_executor import app

    import redis

    with app.connection_for_write() as connection:
        channel = connection.default_channel
        # Все команды — по полному имени через клиент без префикса на том же пуле. Клиент канала
        # префикс дописывает сам, но только части команд (kombu 5.6.2: DEL и SREM — да, SCARD и
        # SSCAN — нет), и на альфе dev 30.09 DEL и SREM по короткому имени сет не тронули
        # (124571 → 124598), хотя SCARD по полному имени его видел: чтение и удаление смотрели
        # в разные ключи. Один клиент и одно имя — одна и та же запись
        client = redis.Redis(connection_pool=channel.client.connection_pool)
        full = (getattr(channel, "global_keyprefix", "") or "") + REPLY_BINDINGS_KEY
        count = client.scard(full)
        _, sample = client.sscan(full, 0, count=REPLY_SAMPLE)
        # Запись — routing_key, шаблон и очередь через разделитель kombu; показываем очередь
        sep = getattr(channel, "sep", "\x06\x16")
        out = {"key": full, "count": count,
               "sample": [(m.decode() if isinstance(m, bytes) else m).split(sep)[-1] for m in sample[:REPLY_SAMPLE]]}
        if purge and count:
            # Сервер — чтобы по отчёту было видно, кто отвечает: Redis или прокси перед ним
            try:
                info = client.info("server")
                out["server"] = {k: info.get(k) for k in ("redis_version", "redis_mode", "server_name", "os")}
            except Exception as exc:  # noqa: BLE001 — сведения, не повод падать
                out["server"] = f"{type(exc).__name__}: {exc}"
            out["probe"] = _probe(client, full)
            out["del"] = _reply(client.delete(full))
            left = client.scard(full)
            if left:
                # Альфа dev 30.09 (проба): DEL и SREM брокер не выполняет, UNLINK и SPOP — выполняет
                out["unlink"] = _reply(client.unlink(full))
                left = client.scard(full)
            # DEL ответил, а сет на месте — добираем пачками SSCAN + SREM. Проход по сету, после
            # которого он не уменьшился, — удаление не работает: крутить до предела бессмысленно
            out["srem"] = 0
            cursor, deadline, before = 0, time.monotonic() + PURGE_SREM_SEC, left
            while left and time.monotonic() < deadline:
                cursor, batch = client.sscan(full, cursor, count=PURGE_BATCH)
                if batch:
                    res = client.srem(full, *batch)
                    out.setdefault("srem_raw", repr(res)[:200])
                    out["srem"] += _count(res)
                if not cursor:
                    left = client.scard(full)
                    if not batch or left >= before:
                        break
                    before = left
            # Альфа dev 30.09: SREM и DEL брокер не выполняет вовсе (проба на временном ключе), SPOP
            # — выполняет. SPOP снимает случайные записи, среди них может оказаться привязка живого
            # опрашивающего: он не получит один ответ и заведёт привязку заново. Цена приемлема
            if left and time.monotonic() < deadline:
                out["spop"] = 0
                while left and time.monotonic() < deadline:
                    popped = client.spop(full, PURGE_BATCH)
                    if not popped:
                        break
                    out["spop"] += len(popped) if isinstance(popped, (list, set)) else 1
                    now_left = client.scard(full)
                    if now_left >= left:
                        break
                    left = now_left
            out["deleted"] = count - left
            out["left"] = left
            logger.warning("📮 сет %s: было %s, DEL=%s, UNLINK=%s, SREM=%s, SPOP=%s, осталось %s",
                           full, count, out["del"], out.get("unlink"), out["srem"], out.get("spop"), left)
    return out


def conclusions(sched: dict, cap: dict, broker: dict, p: dict, pidbox: dict = None) -> list:
    """Вывод словами по правилам из шапки. Пустой список — очередь в норме."""
    out = []
    stale_min = p["stale_min"]
    if sched:
        for d in sched.get("dags", []):
            n = d["causes"].get("лимит дага")
            if n:
                out.append(f"🚧 `{d['dag_id']}` упёрся в `max_active_tasks`={d['max_active_tasks']} "
                           f"(в работе {d['active']}), ждут {d['waiting']}, старейшая {d['oldest_min']} мин — "
                           "поднимать лимит в коде дага, а не `parallelism`")
        pools = {c: n for c, n in sched.get("by_cause", {}).items() if c.startswith("пул ")}
        for c, n in pools.items():
            out.append(f"🏊 {c.capitalize()} исчерпан, ждут слота: {n}")
        behind = sched.get("by_cause", {}).get("окно")
        if behind:
            win = sched["window"]
            names = ", ".join(f"`{d}` {n}" for d, n in win["blocked_dags"][:5])
            out.append(f"🧱 Окно шедулера забито: из первых {win['size']} задач по весу и дате рана "
                       f"{win['blocked']} — даги на своём лимите ({names}); за окном ждут дольше {stale_min} мин "
                       f"{behind} и не рассматриваются вовсе — цикл кончается, как только в окне нашлось что "
                       "запустить. Лечится шириной окна (`parallelism`, `max_tis_per_query`), лимитами этих дагов "
                       "или весом срочных (`priority_weight` + `weight_rule='absolute'`); ещё шедулер тут не "
                       "поможет — очередь ставит один за раз")
        starving = [d for d in sched.get("dags", []) if d["causes"].get("приоритет")]
        if starving:
            names = ", ".join(f"`{d['dag_id']}` (вес {d['weight']}, {d['oldest_min']} мин)" for d in starving[:5])
            out.append(f"⚖️ Ждут дольше {stale_min} мин при месте в даге и пуле, вес ниже медианы ушедших в работу "
                       f"за час ({sched.get('median_dispatched')}): {names} — приоритет; "
                       "`priority_weight` + `weight_rule='absolute'`")
        other = sched.get("by_cause", {}).get("прочее")
        if other:
            out.append(f"❓ Ждут дольше {stale_min} мин без видимой причины (лимит дага, пул, окно, приоритет — нет): "
                       f"{other} — смотреть лог шедулера")
        timeout = sched.get("timeout_runs") or []
        if timeout:
            dags = sorted({r["dag_id"] for r in timeout})
            out.append(f"⏱️ Ранов за сутки, закрытых без единого старта задачи (похоже на `dagrun_timeout`): "
                       f"{len(timeout)} — {', '.join(f'`{d}`' for d in dags[:5])}; это след голодания, см. выше")
        paused = sched.get("paused_runs") or 0
        if paused:
            out.append(f"⏸️ Ранов запаузенных дагов в `queued`/`running`: {paused} — не поедут никогда, "
                       "закрывает `tools_paused_runs_cleanup`")
        if sched.get("parked"):
            out.append(f"🅿️ Задач `scheduled` на парковке (даг на паузе или ран не `running`): {sched['parked']} — "
                       "шедулер их не берёт, в ёмкость не входят")
    if cap:
        if cap.get("open") is not None and cap["open"] <= 0:
            out.append(f"🔒 Слоты executor'а заняты: `queued`+`running` {cap['queued_running']} из "
                       f"{cap['capacity']} (`parallelism` {cap['parallelism']} × шедулеров {cap['schedulers']}) — "
                       "смотреть `slot_reconciler` в логе шедулера")
        pods = cap.get("pods") or {}
        if pods.get("slots_total") and pods.get("slots_busy", 0) >= pods["slots_total"]:
            out.append(f"🔒 Все слоты воркеров заняты: {pods['slots_busy']} из {pods['slots_total']}")
        if broker and broker.get("live") is not None and cap.get("queued", 0) > broker["live"] + (pods.get("slots_busy") or 0):
            out.append(f"📭 В `queued` {cap['queued']} задач, а живых сообщений в брокере {broker['live']}"
                       + (f" и занятых слотов {pods['slots_busy']}" if pods.get("slots_busy") is not None else "")
                       + " — часть задач без сообщения, кандидаты в stuck in queued")
    if broker and broker.get("total"):
        share = broker["junk"] / broker["total"]
        if share >= p["min_junk_share"]:
            out.append(f"🗑️ Мусора в брокере {broker['junk']} из {broker['total']} ({share:.0%}) — можно `purge`")
    pidbox = pidbox or {}
    n = pidbox.get("left", pidbox.get("count")) or 0
    if n >= p["max_reply_bindings"]:
        out.append(f"📮 Привязок ответных очередей в брокере {n} (порог {p['max_reply_bindings']}) — каждый ответ "
                   "воркера на ping/inspect читает их все, и на тысячах главный цикл воркера стоит: задачи не "
                   "берутся, ping молчит, liveness перезапускает поды. Запустить с `purge_pidbox`")
    return out


@dag(
    doc_md=__doc__,
    description='Почему задачи ждут в очереди: брокер, шедулер, ёмкость воркеров; по галке чистка брокера',
    owner_links={"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"},
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. test_connections.py).
        # Разбор очереди нужен ровно тогда, когда она стоит: ждать в ней самому незачем
        "priority_weight": 900,
        "weight_rule": "absolute",
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": on_callback,
    },
    start_date=datetime(2026, 9, 10, tzinfo=MSK),
    tags=["DataTools", "tools", "health"],
    catchup=False,
    # Плагин здоровья: на паузе core пишет «нет отчёта», поэтому включается сам
    is_paused_upon_creation=False,
    max_active_runs=1,
    # Удаление из брокера по таймеру не случается: purge разовая и не сохраняется, так что
    # плановый запуск только разбирает
    schedule=saved_schedule(SAVED, DEFAULTS["schedule"], PARAMS_VAR),
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=on_callback,
    params={
        "stale_min": Param(
            _cfg["stale_min"], type="integer", minimum=1,
            description="Ждёт дольше — «давно ждёт», мин",
        ),
        "queues": Param(
            _cfg["queues"], type=["string", "null"],
            description="Очереди брокера через запятую; пусто — собрать из конфигурации и метабазы",
        ),
        "min_junk_share": Param(
            _cfg["min_junk_share"], type="number", minimum=0, maximum=1,
            description="Не удалять, если доля мусора в очереди меньше этой",
        ),
        "max_delete": Param(
            _cfg["max_delete"], type="integer", minimum=1,
            description="Не удалять, если мусора больше этого числа",
        ),
        "schedule": Param(
            _cfg["schedule"], type=["string", "null"],
            description="cron или пресет (@hourly); пусто — только вручную. Применяется со следующего разбора",
        ),
        "save_params": Param(
            False, type="boolean",
            description=f"Записать значения формы (кроме purge) в Variable {PARAMS_VAR}",
        ),
        "max_reply_bindings": Param(
            _cfg["max_reply_bindings"], type="integer", minimum=1,
            description="Привязок ответных очередей больше этого — вывод 📮 и ⚠️",
        ),
        # Разовое действие, а не настройка: в переменную не пишется и берётся всегда из кода
        "purge": Param(
            False, type="boolean",
            description="Удалить размеченный мусор из брокера. В Variable не сохраняется",
        ),
        "purge_pidbox": Param(
            False, type="boolean",
            description="Удалить сет привязок ответных очередей control-канала. В Variable не сохраняется",
        ),
    },
)
def tools_queue_analyze():

    @task(task_id="params")
    def save_params(**context) -> str:
        """💾 Сохраняет форму (кроме purge) как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context, one_shot=ONE_SHOT)

    # Своя попытка сверх нуля: broker ничего не удаляет, а метабаза на дэве рвёт соединения
    # сама по себе (падает праймери, реплика read-only). Прогон делает человек, и терять его
    # из-за обрыва в середине опроса состояний обидно.
    @task(task_id="broker", retries=1, trigger_rule=TriggerRule.NONE_FAILED)
    def broker(**context) -> dict:
        """📨 Обход очередей брокера, разметка по метабазе и дамп мусора в S3."""
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow import settings
        from sqlalchemy import text

        p = context["params"]
        names = [n.strip() for n in (p["queues"] or "").split(",") if n.strip()]
        if not names:
            names = {conf.get("operators", "default_queue", fallback="default")}
            with settings.engine.connect() as conn:
                names.update(q for (q,) in conn.execute(text(SQL_QUEUES)) if q)
            names = sorted(names)
        logger.info("очереди: %s", ", ".join(names))

        msgs, prefix, keys = _read_queues(names)
        logger.info("префикс ключей: %r, сообщений: %s", prefix, len(msgs))

        # Разбор тел до опроса метабазы: битое сообщение не должно рушить прогон — его
        # цель не прочитается, и оно останется в очереди как неопознанное.
        targets = []
        for m in msgs:
            try:
                m["info"] = _decode(m["raw"])
            except Exception as exc:
                logger.warning("сообщение не разобрано: %s", exc)
                m["info"] = {"id": None, "task": None, "command": None}
            key = _target(m["info"].get("command"))
            if key is not None:
                targets.append(key)

        status, payload = classify(msgs, _states(sorted(set(targets))))
        if status == "skip":
            add_note(payload["note"], context=context, level="Task", title="📨 брокер")
            raise AirflowSkipException(payload["note"])

        junk, live = payload["junk"], payload["live"]

        # Дамп пишем всегда, в том числе при подсчёте без удаления: он и след разбора, и
        # единственный способ вернуть сообщение обратно. В /tmp пода его класть нельзя —
        # под перезапустят, и возвращать будет нечего.
        now = datetime.now(timezone.utc)
        dump_key = f"{PREFIX}{now:%Y-%m-%d}/{now:%H%M%S}.json"
        dump = [
            {
                "key": m["key"],
                "queue": m["queue"],
                "priority": m["priority"],
                "celery_id": m["info"].get("id"),
                "command": m["info"].get("command"),
                "payload": m["raw"].decode() if isinstance(m["raw"], bytes) else m["raw"],
            }
            for m in junk
        ]
        S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).load_string(
            json.dumps({"platform": env_platform(), "stand": env_stand(), "messages": dump},
                       ensure_ascii=False, indent=2),
            key=dump_key, bucket_name=BUCKET_NAME, replace=True,
        )
        logger.info("💾 дамп мусора: s3://%s/%s (%s)", BUCKET_NAME, dump_key, len(dump))

        snapshot = {
            "prefix": prefix,
            "queues": names,
            "keys": keys,
            "total": len(msgs),
            "live": len(live),
            "junk": len(junk),
            "unparsed": payload["unparsed"],
            "eid_mismatch": payload["eid_mismatch"],
            "reasons": payload["reasons"],
            "dump_key": dump_key,
            # Первые несколько мусорных — чтобы по отчёту было видно, что именно размечено
            "sample": [
                {"celery_id": m["info"].get("id"), "command": (m["info"].get("command") or [])[3:6]}
                for m in junk[:5]
            ],
        }
        add_note(
            "\n".join(
                [f"очередей: {len(names)} · сообщений: {len(msgs)} · живых: {len(live)} · мусора: {len(junk)}"]
                + [f"- {k}: {v}" for k, v in sorted(payload['reasons'].items(), key=lambda x: -x[1])]
            ),
            context=context, level="Task", title="📨 брокер",
        )
        return snapshot

    @task(task_id="scheduler", retries=1, trigger_rule=TriggerRule.NONE_FAILED)
    def scheduler(**context) -> dict:
        """🗓️ Почему задачи ждут в scheduled: лимит дага, пул, приоритет; следы голодания."""
        from sqlalchemy import and_, exists, func

        from airflow.jobs.job import Job
        from airflow.models import DagModel, DagRun, Pool, TaskInstance as TI
        from airflow.utils import timezone as tz
        from airflow.utils.session import create_session

        p = context["params"]
        now = tz.utcnow()
        # Себя не считаем: ран разбора на паузе (dags test, ручной запуск) дал бы «припаркованные»
        me = context["dag"].dag_id
        with create_session() as session:
            waiting = [r._asdict() for r in (
                session.query(TI.dag_id, TI.pool, TI.priority_weight, TI.updated_at,
                              DagModel.is_paused, DagModel.max_active_tasks, DagRun.state.label("run_state"),
                              DagRun.execution_date.label("logical_date"))
                .outerjoin(DagModel, DagModel.dag_id == TI.dag_id)
                .outerjoin(DagRun, and_(DagRun.dag_id == TI.dag_id, DagRun.run_id == TI.run_id))
                .filter(TI.state == "scheduled", TI.dag_id != me)
                # В порядке шедулера: при обрезке по лимиту остаётся голова — окно
                .order_by(TI.priority_weight.desc(), DagRun.execution_date)
                .limit(SCHEDULED_LIMIT)
            )]
            active = dict(session.query(TI.dag_id, func.count())
                          .filter(TI.state.in_(("queued", "running"))).group_by(TI.dag_id))
            used = dict(session.query(TI.pool, func.sum(TI.pool_slots))
                        .filter(TI.state.in_(("queued", "running"))).group_by(TI.pool))
            pools = {name: {"slots": slots, "used": int(used.get(name) or 0)}
                     for name, slots in session.query(Pool.pool, Pool.slots)}
            # Через job, а не по start_date: индекса по start_date у task_instance нет, и отбор
            # читал всю таблицу (стенд 27.09.2026: 74 мс на 102 тыс. строк). У задачи,
            # стартовавшей за час, и хартбит её LocalTaskJob свежее часа — job_type_heart
            hour_ago = now - timedelta(hours=1)
            dispatched = [w for (w,) in session.query(TI.priority_weight)
                          .join(Job, Job.id == TI.job_id)
                          .filter(Job.job_type == "LocalTaskJob", Job.latest_heartbeat >= hour_ago,
                                  TI.start_date >= hour_ago, TI.dag_id != me)]

            # След голодания: ран закрыт за сутки, ни одна задача не стартовала, есть skipped.
            # Так выглядит dagrun_timeout, сработавший, пока задачи ждали в scheduled
            started = exists().where(and_(TI.dag_id == DagRun.dag_id, TI.run_id == DagRun.run_id,
                                          TI.start_date.isnot(None)))
            skipped = exists().where(and_(TI.dag_id == DagRun.dag_id, TI.run_id == DagRun.run_id,
                                          TI.state == "skipped"))
            timeout_runs = [
                {"dag_id": d, "run_id": r, "end": e.isoformat() if e else None}
                for d, r, e in session.query(DagRun.dag_id, DagRun.run_id, DagRun.end_date)
                .filter(DagRun.state == "failed", DagRun.end_date >= now - timedelta(hours=24),
                        ~started, skipped)
                .order_by(DagRun.end_date.desc()).limit(200)
            ]
            paused_runs = (session.query(func.count())
                           .select_from(DagRun).join(DagModel, DagModel.dag_id == DagRun.dag_id)
                           .filter(DagModel.is_paused.is_(True), DagRun.state.in_(("queued", "running")),
                                   DagRun.dag_id != me)
                           .scalar())

        out = scheduler_causes(waiting, active, pools, dispatched, now, p["stale_min"], window=_scheduler_window())
        out.update(timeout_runs=timeout_runs, paused_runs=paused_runs, truncated=len(waiting) >= SCHEDULED_LIMIT,
                   pools={k: v for k, v in pools.items() if v["slots"] >= 0 and v["used"] >= v["slots"]})
        lines = [f"`{d['dag_id']}`: ждут {d['waiting']} (давно {d['stale']}, старейшая {d['oldest_min']} мин) · "
                 f"в работе {d['active']}/{d['max_active_tasks']} · вес {d['weight']} · "
                 + (", ".join(f"{k} {v}" for k, v in d["causes"].items()) or "причин нет")
                 for d in out["dags"]]
        summary = (f"живых {out['live']} (давно {out['stale']}), припарковано {out['parked']}; "
                   f"закрыто без старта {len(timeout_runs)}; ранов на паузе {paused_runs}")
        Feed(context, "🗓️ scheduled").done(summary, keep=lines)
        return out

    @task(task_id="capacity", retries=1, trigger_rule=TriggerRule.NONE_FAILED)
    def capacity(**context) -> dict:
        """🔋 Ёмкость: parallelism × живые шедулеры против queued+running; слоты воркеров."""
        from sqlalchemy import func

        from airflow.jobs.job import Job
        from airflow.models import TaskInstance as TI
        from airflow.utils import timezone as tz
        from airflow.utils.session import create_session

        now = tz.utcnow()
        # Как считает сам Airflow: шедулер жив, пока хартбит свежее этого порога
        threshold = conf.getint("scheduler", "scheduler_health_check_threshold", fallback=30)
        with create_session() as session:
            counts = dict(session.query(TI.state, func.count())
                          .filter(TI.state.in_(("queued", "running"))).group_by(TI.state))
            schedulers = (session.query(func.count()).select_from(Job)
                          .filter(Job.job_type == "SchedulerJob", Job.state == "running",
                                  Job.latest_heartbeat >= now - timedelta(seconds=threshold))
                          .scalar())
        parallelism = conf.getint("core", "parallelism")
        queued, running = counts.get("queued", 0), counts.get("running", 0)
        out = {"parallelism": parallelism, "schedulers": schedulers, "queued": queued, "running": running,
               "queued_running": queued + running,
               "capacity": parallelism * schedulers if parallelism else None}
        out["open"] = out["capacity"] - out["queued_running"] if out["capacity"] else None

        # Отбивки воркеров (etl-core hrp_adapter.health_beacon): есть не на каждом контуре
        # и не в каждой версии пакета — нет, так нет
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from sber_app_dataplatform_etl_core.hrp_adapter import health_beacon as hb

            base = conf.get("logging", "REMOTE_BASE_LOG_FOLDER")
            pods = hb.summarize_pods(hb.read_snapshot(S3Hook(aws_conn_id=AWS_CONN_ID, verify=False), base))
            out["pods"] = {k: pods.get(k) for k in ("alive", "silent", "slots_total", "slots_busy", "age_sec")}
            out["pods"]["queues"] = pods.get("queues")
        except Exception as exc:  # noqa: BLE001 — разбор ёмкости без отбивок всё равно полезен
            out["pods_error"] = f"{type(exc).__name__}: {exc}"[:200]

        pods = out.get("pods") or {}
        lines = [f"queued {queued} · running {running} · parallelism (воркер) {parallelism} × шедулеров "
                 f"{schedulers} = {out['capacity']} · свободно {out['open']}"]
        lines.append(f"воркеры: живых {pods.get('alive')}, слоты {pods.get('slots_busy')}/{pods.get('slots_total')}"
                     if pods else f"отбивок воркеров нет: {out.get('pods_error')}")
        add_note("\n".join(lines), context=context, level="Task", title="🔋 ёмкость")
        return out

    @task(task_id="purge", trigger_rule=TriggerRule.NONE_FAILED)
    def purge(snapshot: dict = None, **context) -> dict:
        """🧹 Удаляет размеченный мусор. Только при purge=True."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.celery.executors.celery_executor import app

        p = context["params"]
        if not p["purge"]:
            raise AirflowSkipException("purge=False — очередь не трогаем")

        # Разметки может не быть вовсе: NONE_FAILED считает пропуск успехом, и на пустой
        # очереди (broker пропустился) сюда приходит пустой аргумент. Это не ошибка —
        # удалять нечего, поэтому пропуск, а не падение. Упавший broker сюда не пускает
        # само правило: у него состояние upstream_failed.
        if not snapshot:
            raise AirflowSkipException("разметки нет — broker пропустился, очередь была пуста")

        total, junk = snapshot["total"], snapshot["junk"]
        if not junk:
            raise AirflowSkipException("мусора не размечено — удалять нечего")

        # Пороги проверяем здесь, а не в broker: разметка должна отработать и показать
        # числа даже там, где удалять нельзя.
        share = junk / total if total else 0

        def blocked(reason: str):
            """Причина отказа — в XCom, до падения: иначе отчёт покажет «не удаляли».

            Разница существенная: «не удаляли» — это выключенная галка, а здесь удаление
            запрашивали и порог его не пустил. Возврат таска до отчёта не доходит.
            """
            context["ti"].xcom_push(key="blocked", value=reason)
            return AirflowFailException(reason)

        if share < p["min_junk_share"]:
            raise blocked(
                f"мусора {junk} из {total} — доля {share:.2f} ниже порога {p['min_junk_share']}. "
                f"Картина не та, для которой инструмент сделан: разбираться нужно руками."
            )
        if junk > p["max_delete"]:
            raise blocked(
                f"мусора {junk}, предел за один запуск — {p['max_delete']}. "
                f"Либо поднять предел осознанно, либо сначала проверить разметку по дампу."
            )

        dump = json.loads(
            S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).read_key(snapshot["dump_key"], BUCKET_NAME)
        )
        # С v3.9 дамп — объект с меткой контура, сообщения в "messages"; до того — список
        if isinstance(dump, dict):
            dump = dump["messages"]
        removed, missing = 0, 0
        with app.connection_for_write() as connection:
            client = connection.default_channel.client
            for item in dump:
                # LREM префикса не получает — в dump лежит уже полный ключ (см. _read_queues).
                # Удаляем по точному телу: сообщение, успевшее уйти из очереди само,
                # просто не найдётся, и это не ошибка.
                count = client.lrem(item["key"], 0, item["payload"])
                removed += count
                missing += 1 if not count else 0
        logger.warning("🧹 удалено %s, не найдено %s", removed, missing)

        # Остаток считаем по коротким именам: LLEN префикс получает от клиента сам,
        # а в snapshot["keys"] лежат полные — те, что нужны LRANGE и LREM.
        left = {}
        with app.connection_for_read() as connection:
            channel = connection.default_channel
            for item in snapshot["keys"]:
                short = item["key"][len(snapshot["prefix"]):] if snapshot["prefix"] else item["key"]
                left[short] = channel.client.llen(short)

        result = {"removed": removed, "missing": missing, "left": sum(left.values())}
        add_note(
            f"удалено: {removed} · не найдено: {missing} · осталось в очередях: {result['left']}",
            context=context, level="Task", title="🧹 удаление",
        )
        return result

    @task(task_id="pidbox", retries=1, trigger_rule=TriggerRule.NONE_FAILED)
    def pidbox(**context) -> dict:
        """📮 Привязки ответных очередей control-канала: сколько их и пять для примера."""
        res = reply_bindings()
        text = f"привязок ответных очередей: {res['count']}"
        if res["sample"]:
            text += "\n" + "\n".join(f"- `{q}`" for q in res["sample"])
        add_note(text, context=context, level="Task", title="📮 pidbox")
        return res

    @task(task_id="purge_pidbox", trigger_rule=TriggerRule.NONE_FAILED)
    def purge_pidbox(**context) -> dict:
        """🧹 Удаляет сет привязок ответных очередей. Только при purge_pidbox=True."""
        # Отдельным таском, а не флагом внутри `pidbox`: по сетке видно, запускалась ли чистка
        # и упала ли она отдельно от подсчёта (ИФТ 01.10.2026: `purge` упал, а чистку привязок,
        # прошедшую внутри `pidbox`, приняли за ту же неудачу).
        from airflow.exceptions import AirflowSkipException

        if not context["params"]["purge_pidbox"]:
            raise AirflowSkipException("purge_pidbox=False — привязки не трогаем")
        res = reply_bindings(purge=True)
        text = f"привязок было: {res['count']}"
        if "left" in res:
            text += (f" · удалено {res['deleted']}, осталось {res['left']} (DEL ответил {res['del']}"
                     + (f", UNLINK {res['unlink']}" if "unlink" in res else "") + f", SREM {res['srem']}"
                     + (f", SPOP {res['spop']}" if "spop" in res else "") + ")")
            text += f"\n\nсервер: `{res.get('server')}`" + (f"\nпервый ответ SREM: `{res['srem_raw']}`" if res.get("srem_raw") else "") \
                + (f"\nпроба удалений: `{res['probe']}`" if res.get("probe") else "")
        add_note(text, context=context, level="Task", title="🧹 привязки")
        return res

    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE)
    def report(snapshot: dict = None, sched: dict = None, cap: dict = None, purged: dict = None,
               bindings: dict = None, cleared: dict = None, **context) -> str:
        """📊 Выводы словами и сводка по разделам."""
        # Запускается при любом исходе (`ALL_DONE`), поэтому любого словаря может не быть:
        # XCom упавшего или пропущенного таска не существует, и аргумент приезжает пустым.
        # Своё падение здесь хуже отсутствия сводки — дежурный получит два красных таска
        # вместо объяснения, что случилось с первым.
        p = context["params"]
        purged = purged or {}
        dag_run = context["dag_run"]

        def state(task_id):
            return getattr(dag_run.get_task_instance(task_id), "state", None)

        cleared = cleared or {}
        # Вывод 📮 — по тому, что осталось после чистки (left), а не по счёту до неё
        found = conclusions(sched or {}, cap or {}, snapshot or {}, p, {**(bindings or {}), **cleared})
        lines = ["**Выводы:**"] + [f"- {x}" for x in found] if found else ["**Выводы:** ✅ очередь в норме"]

        # Отказ порога отличается от выключенной галки: удаление запрашивали, и его не
        # пустило. Причина приезжает из XCom, потому что возврат упавшего таска до сюда
        # не доходит.
        stop = context["ti"].xcom_pull(task_ids="purge", key="blocked")
        if purged.get("removed") is not None:
            deleted = purged["removed"]
        elif stop:
            deleted = "⛔️ порог не пустил"
        else:
            deleted = "☮️ не удаляли"

        lines.append("")
        if sched:
            causes = ", ".join(f"{k} {v}" for k, v in sched["by_cause"].items()) or "—"
            lines.append(f"🗓️ scheduled: живых {sched['live']}, давно ждут {sched['stale']} ({causes}), "
                         f"припарковано {sched['parked']}")
            lines.append(f"⚖️ приоритет: ушло в работу за час {sched['dispatched_1h']}, медиана веса "
                         f"{sched['median_dispatched']}")
        else:
            lines.append(f"🗓️ scheduled: ❌ разбор не отработал ({state('scheduler')})")
        if cap:
            lines.append(f"🔋 ёмкость: queued {cap['queued']} + running {cap['running']} из {cap['capacity']} "
                         f"(parallelism воркера {cap['parallelism']} × шедулеров {cap['schedulers']})")
            pods = cap.get("pods")
            lines.append(f"👷 воркеры: живых {pods['alive']}, слоты {pods['slots_busy']}/{pods['slots_total']}"
                         if pods else f"👷 воркеры: отбивок нет: {cap.get('pods_error')}")
        else:
            lines.append(f"🔋 ёмкость: ❌ не отработала ({state('capacity')})")
        if snapshot:
            lines += [
                f"📨 брокер: сообщений {snapshot['total']}, живых {snapshot['live']}, мусора {snapshot['junk']}",
                f"🧹 удалено: {deleted} · осталось {purged.get('left', '—')}",
                f"🗑️ дамп мусора: `s3://{BUCKET_NAME}/{snapshot['dump_key']}`",
            ]
        else:
            s = state("broker")
            lines.append("📨 брокер: очередь пуста" if s == "skipped" else f"📨 брокер: ❌ не отработал ({s})")
        if bindings:
            lines.append(f"📮 привязки ответов: {bindings['count']}")
        else:
            lines.append(f"📮 привязки ответов: ❌ не прочитаны ({state('pidbox')})")
        if "left" in cleared:
            lines.append(f"🧹 чистка привязок: удалено {cleared['deleted']}, осталось {cleared['left']}")
        elif state("purge_pidbox") == "failed":
            lines.append("🧹 чистка привязок: ❌ упала — лог таска `purge_pidbox`")
        if stop:
            lines += ["", f"> ⛔️ {stop}"]
        if snapshot and snapshot.get("reasons"):
            lines += ["", "**Разметка брокера:**"]
            lines += [f"- {k}: {v}" for k, v in sorted(snapshot["reasons"].items(), key=lambda x: -x[1])]

        # Разбор целиком — рядом с дампом мусора: сравнивать запуски между собой
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            now = datetime.now(timezone.utc)
            key = f"{PREFIX}{now:%Y-%m-%d}/{now:%H%M%S}_analyze.json"
            S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).load_string(
                json.dumps({"platform": env_platform(), "stand": env_stand(), "conclusions": found, "scheduler": sched, "capacity": cap,
                            "broker": {k: v for k, v in (snapshot or {}).items() if k != "keys"},
                            "purge": purged, "pidbox": bindings,
                            "purge_pidbox": cleared or None}, ensure_ascii=False, indent=2, default=str),
                key=key, bucket_name=BUCKET_NAME, replace=True,
            )
            lines.append(f"\nРазбор целиком: `s3://{BUCKET_NAME}/{key}`")
        except Exception as exc:  # noqa: BLE001 — сводка важнее дампа
            logger.warning("дамп разбора не записан: %s", exc)

        summary = "\n".join(lines)
        logger.info("📊 сводка:\n%s", summary)
        title = f"🔬 Разбор очереди: {len(found)} выводов" if found else "🔬 Разбор очереди: в норме"
        add_note(summary, context=context, level="DAG,Task", title=title)

        # Вердикт не выше warn: очередь — не авария. Не отработавший сборщик — тоже warn
        warns = [x for x in found if x.startswith(WARN_MARKS)]
        warns += [f"{t} не отработал ({state(t)})" for t in ("broker", "scheduler", "capacity", "pidbox")
                  if state(t) in ("failed", "upstream_failed")]
        push_health({"queue": {"status": "warn" if warns else "healthy",
                               "summary": "; ".join(warns) if warns else f"в норме, выводов {len(found)}",
                               "conclusions": found}}, context)
        return summary

    done = save_params()
    snapshot, sched, cap, bindings = broker(), scheduler(), capacity(), pidbox()
    done >> [snapshot, sched, cap, bindings]
    purged = purge(snapshot)
    # После подсчёта: в отчёте и «сколько было», и «сколько осталось»
    cleared = purge_pidbox()
    bindings >> cleared
    report(snapshot, sched, cap, purged, bindings, cleared) >> health_tasks(ttl_sec=REPORT_TTL_SEC, skill=SKILL)


tools_queue_analyze()
