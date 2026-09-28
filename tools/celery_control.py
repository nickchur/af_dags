"""### 📡 Control-канал celery: доходят ли ping и inspect через брокер
*2026-09-28 08:25 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Задачи ходят через списки Redis (LPUSH/BRPOP), а `ping`, `inspect`, `list-workers` — через
pub/sub (control-канал `pidbox`). Второе может не работать при живом первом: воркеры берут
задачи, но никому не отвечают. От control-канала зависят liveness-проба воркера
(`worker_health_api`, перезапускает под после 20 минут молчания) и число воркеров в
карточке Health (`health_beacon` спрашивает свой узел). 28.09.2026 на dev так и было:
`celery list-workers` из пода воркера — пусто, при восьми работающих воркерах.

Запуск вручную, параметров нет. Таск `check` исполняется на воркере — python и доступ к
брокеру есть только там.

| Поле | Что значит |
|---|---|
| `node` | узел Redis, к которому привязан транспорт (у кластера — владелец слота `global_keyprefix`) |
| `acl_whoami`, `acl_user` | пользователь брокера и его права; нет `&*` в каналах — pub/sub закрыт ACL |
| `pidbox_channels` | каналы `*pidbox*` с прямыми подписчиками; обычно пусто — kombu подписывается шаблоном |
| `pattern_subs` | число подписок-шаблонов на узле (`PUBSUB NUMPAT`); 0 — воркеры не подписаны на control-канал |
| `loopback` | публикация самому себе на том же соединении; `received: None` — pub/sub не работает вообще |
| `ping` | ответы воркеров на `app.control.ping`: пусто — control-канал не работает |

Пустой `ping` роняет таск: так результат виден в сетке без открытия заметки.
"""

from datetime import datetime, timedelta, timezone
import logging
import time

from airflow.decorators import dag, task

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore

logger = logging.getLogger("airflow.task")

ensure_pool(TOOLS_POOL)

PING_TIMEOUT_SEC = 5


def _plain(value):
    """Ответы redis приходят байтами, в том числе ключи словарей; XCom и заметке нужны строки."""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {_plain(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _try(fn):
    """Значение или текст ошибки: одна неудавшаяся проверка не должна прятать остальные."""
    try:
        return _plain(fn())
    except Exception as exc:  # noqa: BLE001 - нужен текст любой ошибки
        return f"ERROR {type(exc).__name__}: {exc}"


@dag(
    doc_md=__doc__,
    owner_links={"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"},
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 0,
        "priority_weight": 900,
        "weight_rule": "absolute",
        "execution_timeout": timedelta(minutes=5),
        "on_failure_callback": on_callback,
    },
    start_date=datetime(2026, 9, 10, tzinfo=timezone.utc),
    tags=["DataLab", "tools"],
    catchup=False,
    max_active_runs=1,
    schedule=None,
    on_failure_callback=on_callback,
)
def tools_celery_control():

    @task(task_id="check")
    def check(**context) -> dict:
        from airflow.providers.celery.executors.celery_executor import app

        out = {}
        # Клиента берём у канала kombu: он уже привязан к нужному узлу кластера
        with app.connection_for_read() as conn:
            ch = conn.default_channel
            cli = ch.client
            kw = cli.connection_pool.connection_kwargs
            out["node"] = f"{kw.get('host')}:{kw.get('port')}"
            out["global_keyprefix"] = ch.global_keyprefix
            out["keyprefix_fanout"] = ch.keyprefix_fanout
            out["acl_whoami"] = _try(lambda: cli.execute_command("ACL", "WHOAMI"))
            out["acl_user"] = _try(lambda: cli.execute_command("ACL", "GETUSER", out["acl_whoami"]))

            def channels():
                names = cli.pubsub_channels("*pidbox*")
                return dict(cli.pubsub_numsub(*names)) if names else {}

            out["pidbox_channels"] = _try(channels)
            # kombu подписывает воркеры шаблоном (fanout_patterns): PUBSUB CHANNELS их не видит
            out["pattern_subs"] = _try(lambda: cli.pubsub_numpat())

            def loopback():
                ps = cli.pubsub()
                try:
                    ps.subscribe("tools_celery_control.loopback")
                    ps.get_message(timeout=2)  # подтверждение подписки
                    receivers = cli.publish("tools_celery_control.loopback", "ping")
                    return {"receivers": receivers, "received": ps.get_message(timeout=5)}
                finally:
                    ps.close()

            out["loopback"] = _try(loopback)

        started = time.monotonic()
        out["ping"] = _try(lambda: app.control.ping(timeout=PING_TIMEOUT_SEC))
        out["ping_sec"] = round(time.monotonic() - started, 1)

        for key, value in out.items():
            logger.info("%s: %s", key, value)
        ok = isinstance(out["ping"], list) and bool(out["ping"])
        add_note(out, context, title=f"{'✅' if ok else '❌'} control-канал celery")
        if not ok:
            raise RuntimeError(f"ни один воркер не ответил на ping за {PING_TIMEOUT_SEC} с: {out['ping']}")
        return out

    check()


tools_celery_control()
