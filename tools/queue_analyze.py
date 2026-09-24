"""### 🔬 Разбор очереди: почему задачи ждут, и мусор в брокере
*2026-09-24 13:00 MSK · v2.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

До 24.09.2026 — `tools_queue_cleanup` (`queue_cleanup.py`): только разметка и чистка
брокера. Теперь даг в первую очередь **разбирает** очередь — то, что 23–24.09.2026 на сигме
выясняли руками по логу шедулера и ответам health, — а чистка брокера стала одним таском
по галочке `purge`.

**Таски:** после `params` параллельно `broker`, `scheduler`, `capacity`; `purge` (только при
`purge`) ждёт `broker`; `report` — всех, при любом их исходе; `prune` — чистка дампов, сам по себе.

| Таск | Что смотрит |
|---|---|
| `broker` | Сообщения очередей celery: живые и мусор (задача уже в терминальном состоянии или её нет в метабазе). Дамп мусора — в бакет логов |
| `scheduler` | Почему задачи ждут в `scheduled`: лимит дага (`max_active_tasks`), исчерпанный пул, приоритет; раны, закрытые без единого старта; раны запаузенных дагов |
| `capacity` | `queued`+`running` против `parallelism` × живые шедулеры; слоты воркеров из отбивок (`health_beacon`), если они есть на контуре |
| `purge` | Удаление размеченного мусора из брокера — с прежними предохранителями |
| `report` | Заметка на ран: разделы и **вывод словами** |

**Правила вывода** — из разбора 23–24.09.2026:

| Картина | Вывод |
|---|---|
| Даг ждёт, `queued`+`running` у него ≥ `max_active_tasks` | Упёрся в свой лимит: поднимать лимит в коде дага, а не `parallelism`. Сигма 23.09: 297 задач в `scheduled`, `raw_to_stable_*` с лимитом 4, `slot_reconciler gap=0` — слоты не текли |
| Ждёт слот пула, пул занят целиком | Пул исчерпан |
| Ждёт дольше `stale_min`, дагу и пулу есть место, вес ниже медианы ушедших в работу за час | Голодает из-за приоритета: `downstream` даёт большим дагам вес в сотни и тысячи (`tfs_kafka_snd` с весом 1 против 22–1921) — `priority_weight` + `weight_rule='absolute'` |
| Ран failed за сутки, ни одна задача не стартовала, есть `skipped` | Закрыт `dagrun_timeout` без старта — след того же голодания (`tfs_kafka_snd` 12:00–15:00) |
| `queued`+`running` ≥ `parallelism` × шедулеры | Слоты executor'а заняты: смотреть `slot_reconciler` в логе шедулера |
| Мусора в брокере ≥ `min_junk_share` | Можно запускать с `purge` |
| Раны запаузенных дагов в `queued` / `running` | Не поедут никогда — `tools_paused_runs_cleanup` |

`parallelism` здесь — из конфига **воркера**, на котором идёт таск; у шедулера он может быть
другим (vault перекрывает `airflow.cfg`). Значение шедулера даёт MCP `get_config_value` →
`components` (etl-core #71).

| Параметр | Описание |
|---|---|
| `stale_min` | Ждёт дольше — «давно ждёт», мин *(default: `5`, как у карточки Health)* |
| `queues` | Очереди через запятую; пусто — собрать из конфигурации и метабазы *(default: пусто)* |
| `min_junk_share` | Не удалять, если мусора меньше этой доли очереди *(default: `0.5`)* |
| `max_delete` | Не удалять, если мусора больше этого числа *(default: `5000`)* |
| `keep_days` | Сколько дней держим дампы, старше — убираем *(default: `30`)* |
| `schedule` | Расписание; пусто — только вручную *(default: пусто)* |
| `save_params` | Записать форму в Variable `tools_queue_analyze_params` *(default: `False`)* |
| `purge` | Удалять размеченный мусор. **В Variable не сохраняется** *(default: `False`)* |

Пороги прежнего дага (Variable `tools_queue_cleanup_cfg`) берутся значениями по умолчанию,
пока не сохранена новая переменная. Дампы по-прежнему в `queue_cleanup/<YYYY-MM-DD>/`:
`<HHMMSS>.json` — мусор брокера (из него сообщение возвращается `rpush` по `key` и
`payload`), `<HHMMSS>_analyze.json` — разбор целиком, чтобы сравнивать запуски.

Чего `broker` не заберёт — сообщения, которые воркеры уже держат в работе: их в очереди нет.
Они вернутся туда через `visibility_timeout`, и повторный запуск их подберёт.
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
import re

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )

logger = logging.getLogger("airflow.task")

# Пул заводим при разборе файла: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Бакет и коннект — те же, что у логов задач, но папка своя: дампы не должны попасть под
# чистку логов (см. tools/log_cleanup.py) и мешаться с ними в выдаче.
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
    "keep_days": 30,
    # Как STALE_AFTER_SEC карточки Health (etl-core celery_health_plugin): младше — обычное
    # ожидание цикла шедулера
    "stale_min": 5,
    # Разбор ничего не меняет, но и регулярным быть не обязан: по умолчанию — вручную
    "schedule": "",
}
ONE_SHOT = ("purge",)

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
# Длина даты в имени папки дампа (`YYYY-MM-DD`) — по ней же отбираются старые дампы.
DATE_LEN = 10
# Имя папки дампа. Отбор старого идёт строковым сравнением, поэтому объект, имя которого
# на дату не похоже, под чистку попадать не должен — он «меньше» любой даты.
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}/")
# Предел на чтение: тела читаются в память целиком, иначе разметить их нечем.
# Гигабайтная очередь — уже не случай этого инструмента, и упасть на входе честнее, чем
# по OOM в середине разбора. Проверяется по LLEN, до LRANGE, то есть даром, — и по
# сумме тоже: в памяти лежат тела всех очередей сразу, поэтому пять очередей по сорок
# тысяч ничем не лучше одной на двести.
READ_LIMIT = 50_000
# Потолок выборки scheduled: на сигме 23.09 их было 297; десятки тысяч — уже авария,
# и для выводов хватит первой партии (в отчёте помечено)
SCHEDULED_LIMIT = 20_000
# Строк в таблице дагов в заметке
NOTE_ROWS = 30



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
    args = json.loads(body)[0] if body else []
    return {
        "id": headers.get("id"),
        "task": headers.get("task"),
        "command": args[0] if args and isinstance(args[0], list) else None,
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


def scheduler_causes(waiting: list, active: dict, pools: dict, dispatched: list, now, stale_min: int) -> dict:
    """Почему ждут задачи в ``scheduled`` — без Airflow, чтобы проверялось тестом.

    Args:
        waiting: ``[{dag_id, pool, priority_weight, updated_at, is_paused, run_state, max_active_tasks}]``.
        active: ``{dag_id: queued+running}``.
        pools: ``{pool: {'slots': n, 'used': n}}``.
        dispatched: веса задач, стартовавших за последний час.
        now: Текущее время (aware).
        stale_min: Порог «давно ждёт», мин.

    Returns:
        ``{'live', 'parked', 'stale', 'by_cause': {причина: n}, 'dags': [...], 'median_dispatched'}``.
        Причина у давно ждущей живой задачи одна, по порядку проверки: лимит дага → пул →
        приоритет → прочее. Лимит дага первым: при нём шедулер задачу не возьмёт при любом
        весе и любом пуле.
    """
    edge = now - timedelta(minutes=stale_min)
    median = _median(dispatched)
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
            "median_dispatched": median, "dispatched_1h": len(dispatched)}


def conclusions(sched: dict, cap: dict, broker: dict, p: dict) -> list:
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
        starving = [d for d in sched.get("dags", []) if d["causes"].get("приоритет")]
        if starving:
            names = ", ".join(f"`{d['dag_id']}` (вес {d['weight']}, {d['oldest_min']} мин)" for d in starving[:5])
            out.append(f"⚖️ Ждут дольше {stale_min} мин при месте в даге и пуле, вес ниже медианы ушедших в работу "
                       f"за час ({sched.get('median_dispatched')}): {names} — приоритет; "
                       "`priority_weight` + `weight_rule='absolute'`")
        other = sched.get("by_cause", {}).get("прочее")
        if other:
            out.append(f"❓ Ждут дольше {stale_min} мин без видимой причины (лимит дага, пул, приоритет — нет): "
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
    return out


@dag(
    doc_md=__doc__,
    owner_links={"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"},
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. show_connections.py).
        # Разбор очереди нужен ровно тогда, когда она стоит: ждать в ней самому незачем
        "priority_weight": 900,
        "weight_rule": "absolute",
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": on_callback,
    },
    start_date=datetime(2026, 9, 10, tzinfo=timezone.utc),
    tags=["DataLab", "tools", "clean"],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    # Удаление из брокера по таймеру не случается: purge разовая и не сохраняется, так что
    # плановый запуск только разбирает
    schedule=saved_schedule(SAVED, None, PARAMS_VAR),
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
        "keep_days": Param(
            _cfg["keep_days"], type="integer", minimum=1,
            description="Сколько дней держим дампы",
        ),
        "schedule": Param(
            _cfg["schedule"], type=["string", "null"],
            description="cron или пресет (@hourly); пусто — только вручную. Применяется со следующего разбора",
        ),
        "save_params": Param(
            False, type="boolean",
            description=f"Записать значения формы (кроме purge) в Variable {PARAMS_VAR}",
        ),
        # Разовое действие, а не настройка: в переменную не пишется и берётся всегда из кода
        "purge": Param(
            False, type="boolean",
            description="Удалить размеченный мусор из брокера. В Variable не сохраняется",
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
            json.dumps(dump, ensure_ascii=False, indent=2),
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
                              DagModel.is_paused, DagModel.max_active_tasks, DagRun.state.label("run_state"))
                .outerjoin(DagModel, DagModel.dag_id == TI.dag_id)
                .outerjoin(DagRun, and_(DagRun.dag_id == TI.dag_id, DagRun.run_id == TI.run_id))
                .filter(TI.state == "scheduled", TI.dag_id != me)
                .limit(SCHEDULED_LIMIT)
            )]
            active = dict(session.query(TI.dag_id, func.count())
                          .filter(TI.state.in_(("queued", "running"))).group_by(TI.dag_id))
            used = dict(session.query(TI.pool, func.sum(TI.pool_slots))
                        .filter(TI.state.in_(("queued", "running"))).group_by(TI.pool))
            pools = {name: {"slots": slots, "used": int(used.get(name) or 0)}
                     for name, slots in session.query(Pool.pool, Pool.slots)}
            dispatched = [w for (w,) in session.query(TI.priority_weight)
                          .filter(TI.start_date >= now - timedelta(hours=1), TI.dag_id != me)]

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

        out = scheduler_causes(waiting, active, pools, dispatched, now, p["stale_min"])
        out.update(timeout_runs=timeout_runs, paused_runs=paused_runs, truncated=len(waiting) >= SCHEDULED_LIMIT,
                   pools={k: v for k, v in pools.items() if v["slots"] >= 0 and v["used"] >= v["slots"]})
        rows = ["| Даг | Ждут | Давно | Старейшая, мин | В работе / лимит | Вес | Причины |",
                "|---|---:|---:|---:|---|---|---|"]
        for d in out["dags"][:NOTE_ROWS]:
            causes = ", ".join(f"{k} {v}" for k, v in d["causes"].items()) or "—"
            rows.append(f"| `{d['dag_id']}` | {d['waiting']} | {d['stale']} | {d['oldest_min']} | "
                        f"{d['active']} / {d['max_active_tasks']} | {d['weight']} | {causes} |")
        title = (f"🗓️ scheduled: живых {out['live']} (давно {out['stale']}), припарковано {out['parked']}; "
                 f"закрыто без старта {len(timeout_runs)}; ранов на паузе {paused_runs}")
        add_note("\n".join(rows) if out["dags"] else "живых scheduled нет", context=context,
                 level="Task", title=title)
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

    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE)
    def report(snapshot: dict = None, sched: dict = None, cap: dict = None, purged: dict = None,
               **context) -> str:
        """📊 Выводы словами и сводка по разделам.

        Запускается при любом исходе (`ALL_DONE`), поэтому любого словаря может не быть:
        XCom упавшего или пропущенного таска не существует, и аргумент приезжает пустым.
        Своё падение здесь хуже отсутствия сводки — дежурный получит два красных таска
        вместо объяснения, что случилось с первым.
        """
        p = context["params"]
        purged = purged or {}
        dag_run = context["dag_run"]

        def state(task_id):
            return getattr(dag_run.get_task_instance(task_id), "state", None)

        found = conclusions(sched or {}, cap or {}, snapshot or {}, p)
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

        lines += ["", "| Раздел | Итог |", "|---|---|"]
        if sched:
            causes = ", ".join(f"{k} {v}" for k, v in sched["by_cause"].items()) or "—"
            lines.append(f"| scheduled | живых {sched['live']}, давно ждут {sched['stale']} ({causes}), "
                         f"припарковано {sched['parked']} |")
            lines.append(f"| приоритет | ушло в работу за час {sched['dispatched_1h']}, медиана веса "
                         f"{sched['median_dispatched']} |")
        else:
            lines.append(f"| scheduled | ❌ разбор не отработал ({state('scheduler')}) |")
        if cap:
            lines.append(f"| ёмкость | queued {cap['queued']} + running {cap['running']} из {cap['capacity']} "
                         f"(parallelism воркера {cap['parallelism']} × шедулеров {cap['schedulers']}) |")
            pods = cap.get("pods")
            lines.append(f"| воркеры | живых {pods['alive']}, слоты {pods['slots_busy']}/{pods['slots_total']} |"
                         if pods else f"| воркеры | отбивок нет: {cap.get('pods_error')} |")
        else:
            lines.append(f"| ёмкость | ❌ не отработала ({state('capacity')}) |")
        if snapshot:
            lines += [
                f"| брокер | сообщений {snapshot['total']}, живых {snapshot['live']}, мусора {snapshot['junk']} |",
                f"| удалено | {deleted} · осталось {purged.get('left', '—')} |",
                f"| дамп мусора | `s3://{BUCKET_NAME}/{snapshot['dump_key']}` |",
            ]
        else:
            s = state("broker")
            lines.append("| брокер | очередь пуста |" if s == "skipped" else f"| брокер | ❌ не отработал ({s}) |")
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
                json.dumps({"conclusions": found, "scheduler": sched, "capacity": cap,
                            "broker": {k: v for k, v in (snapshot or {}).items() if k != "keys"},
                            "purge": purged}, ensure_ascii=False, indent=2, default=str),
                key=key, bucket_name=BUCKET_NAME, replace=True,
            )
            lines.append(f"\nРазбор целиком: `s3://{BUCKET_NAME}/{key}`")
        except Exception as exc:  # noqa: BLE001 — сводка важнее дампа
            logger.warning("дамп разбора не записан: %s", exc)

        summary = "\n".join(lines)
        logger.info("📊 сводка:\n%s", summary)
        title = f"🔬 Разбор очереди: {len(found)} выводов" if found else "🔬 Разбор очереди: в норме"
        add_note(summary, context=context, level="DAG,Task", title=title)
        return summary

    @task(task_id="prune", trigger_rule=TriggerRule.ALL_DONE)
    def prune(**context) -> str:
        """🧹 Убирает дампы старше `keep_days`.

        Своими руками, а не lifecycle-правилом бакета: наш S3-шлюз не принимает
        PutBucketLifecycleConfiguration (требует Content-MD5, которого boto3 больше не шлёт).
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        keep = int(context["params"]["keep_days"])
        edge = (datetime.now(timezone.utc) - timedelta(days=keep)).strftime("%Y-%m-%d")
        hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX) or []
        # Дата лежит в имени папки, поэтому отбираем строковым сравнением, а не запросом
        # метаданных на каждый объект: формат `YYYY-MM-DD` сравнивается как строка верно.
        # Имя, на дату не похожее, пропускаем: чужой объект под нашим префиксом иначе
        # сравнился бы «меньше края» и был бы удалён заодно.
        old = [
            k for k in keys
            if DATE_DIR_RE.match(k[len(PREFIX):]) and k[len(PREFIX):len(PREFIX) + DATE_LEN] < edge
        ]
        if old:
            hook.delete_objects(bucket=BUCKET_NAME, keys=old)
        msg = f"дампов: {len(keys)}, убрано старше {edge}: {len(old)}"
        logger.info("🧹 %s", msg)
        return msg

    done = save_params()
    snapshot, sched, cap = broker(), scheduler(), capacity()
    done >> [snapshot, sched, cap]
    purged = purge(snapshot)
    report(snapshot, sched, cap, purged)
    prune()


tools_queue_analyze()
