"""### 🗄️ Метабаза Airflow без ORM (`plugins/af_meta.py`)
*2026-09-25 14:48 MSK · v1.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

В Airflow 3 у задачи и у разбора DAG-файла нет ORM-доступа к метабазе: `create_session`
бросает «Direct database access via the ORM is not allowed in Airflow 3.0». Всё, что даги
CTL делали через `DagModel`, `TaskInstance`, `Pool`, `Log` и заметки, здесь делается SQL-ом
по подключению метабазы, а запуск DAG-а — через Execution API, как у `TriggerDagRunOperator`.

Решение 21.09.2026: SQL по подключению `conns.pg` из `ctl_config` (`airflowdb`), без REST и
служебной учётки. Цена — запись в таблицы Airflow мимо его API: SQL здесь привязан к схеме
3.3.1 и сверяется при каждом обновлении Airflow (список таблиц — ниже, у функций).

| Функция | Что делает |
|---|---|
| `sql()` | SQL в метабазе, строки словарями (замена `pg_exe` на `create_session`) |
| `meta_session()` | сессия SQLAlchemy для кода на `session.execute(text(...))` (замена `create_session`) |
| `schema()` | схема метабазы: `main_3` на альфе, `public` на стенде |
| `dag_state()` | пауза, «устаревший» и наличие в `serialized_dag` одним запросом |
| `set_paused()` | пауза DAG-а (было `DagModel.is_paused`) |
| `set_ti_state()` | состояние соседней задачи рана (было `TaskInstance.set_state`) |
| `pool_*()` | пулы: размер, создание, загрузка (было `Pool`, `TaskInstance`) |
| `log_event()` / `last_event()` | журнал `log` (было `Log`) |
| `update_note()` | заметка рана или задачи: прочитал — изменил — записал под блокировкой |
| `trigger_dag_run()` | запуск DAG-а изнутри задачи через Execution API |

Схема: `search_path` = `[database] sql_alchemy_schema` и `public`, имена таблиц без схемы. На
альфе метабаза AF3 в `main_3`, а в `main` рядом лежит метабаза AF2 (для отката) — её в пути
быть не должно, иначе чтение и запись молча уйдут в AF2. На стенде AF3 — `public`.
"""

from contextlib import closing, contextmanager
from datetime import datetime, timezone

from logging import getLogger
logger = getLogger("airflow.task")

META_CONN_DEFAULT = 'airflowdb'
STATEMENT_TIMEOUT_SEC = 60


def search_path() -> str:
    """`main_3,public` на альфе, `public,public` на стенде: схема из настроек Airflow."""
    from airflow.configuration import conf
    return f"{conf.get('database', 'sql_alchemy_schema', fallback='') or 'public'},public"


class DagNotFound(Exception):
    """DAG-а нет в `serialized_dag`: запускать нечего (Execution API ответил 404)."""


class DagRunAlreadyExists(Exception):
    """Ран с таким run_id уже есть (Execution API ответил 409)."""


def meta_conn_id() -> str:
    """Подключение к метабазе: `conns.pg.conn_id` из `ctl_config`, иначе `airflowdb`.

    Импорт внутри: `ctl_utils` сам импортирует `utils`, а тот — этот модуль.
    """
    try:
        from plugins.ctl_utils import get_config  # type: ignore
    except ImportError:
        try:
            from CI06932748.tools.ctl_utils import get_config  # type: ignore
        except ImportError:
            return META_CONN_DEFAULT
    try:
        return (get_config().get('conns', {}).get('pg', {}) or {}).get('conn_id') or META_CONN_DEFAULT
    except Exception:
        return META_CONN_DEFAULT


def sql(query, params=None, timeout=STATEMENT_TIMEOUT_SEC, conn_id=None):
    """Выполняет SQL в метабазе одной транзакцией и возвращает строки списком словарей.

    Запрос без результата (UPDATE/INSERT без RETURNING) даёт пустой список. Без `params`
    psycopg2 строку не форматирует, поэтому `%` в `LIKE '%x%'` можно писать как есть; с
    `params` его надо удваивать. `conn_id` — другое подключение к той же базе (например,
    владельца таблиц из `utils.get_af_conn()` для чистки и `pg_terminate_backend`).
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id=conn_id or meta_conn_id())
    with closing(hook.get_conn()) as conn:
        with conn:  # commit при выходе, rollback при исключении
            with conn.cursor() as cur:
                cur.execute("SELECT set_config('search_path', %s, true)", (search_path(),))
                if timeout:
                    cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{int(timeout)}s",))
                cur.execute(query, params)
                if cur.description is None:
                    return []
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]



_engines = {}


def _engine(conn_id):
    """Движок SQLAlchemy на подключение, один на процесс. NullPool: пул нам не нужен, а
    соединение с выставленным search_path не должно пережить задачу в чужих руках."""
    if conn_id not in _engines:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
        from sqlalchemy.pool import NullPool
        # search_path и в параметрах подключения: каждое новое соединение сразу видит схему
        _engines[conn_id] = PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine(
            engine_kwargs={'poolclass': NullPool,
                           'connect_args': {'options': f'-c search_path={search_path()}'}})
    return _engines[conn_id]


@contextmanager
def meta_session(conn_id=None):
    """Сессия SQLAlchemy к метабазе — замена `airflow.utils.session.create_session`.

    Для кода, который писал `session.execute(text(...), {...})` и сам коммитил порциями
    (`check/db_cleanup.py`): SQL и именованные параметры остаются как были.

    Сессия привязана к ОДНОМУ соединению на всё время. Иначе (сессия на движке с NullPool)
    каждый commit закрывал бы соединение, и следующая порция шла бы в новое — без
    `SET search_path` и без `SET lock_timeout`, которые `db_cleanup` ставит один раз
    (замечание ревью 21.09.2026; стенд его не ловил — там таблицы в `public`, схеме по
    умолчанию). При выходе — commit, при исключении — rollback, как у `create_session`.
    """
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    conn = _engine(conn_id or meta_conn_id()).connect()
    session = Session(bind=conn)
    try:
        session.execute(text("SELECT set_config('search_path', :p, false)"), {'p': search_path()})
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        conn.close()


def schema(conn_id=None) -> str:
    """Схема, в которой лежит метабаза: `main_3` на альфе, `public` на стенде AF3.

    Не `current_schema()`: та отдаёт первую существующую схему из search_path, а на стенде
    AF3 есть пустая `main` рядом с таблицами в `public` (поймано 21.09.2026). Спрашиваем,
    куда search_path разрешает саму `dag_run`.
    """
    return sql("SELECT relnamespace::regnamespace::text AS s FROM pg_class"
               " WHERE oid = 'dag_run'::regclass", conn_id=conn_id)[0]['s']


# === DAG-и ===

def dag_state(dag_id: str) -> dict | None:
    """`{is_paused, is_stale, last_parsed_time, serialized}` или None, если строки в `dag` нет.

    `serialized` — есть ли DAG в `serialized_dag`: без неё запуск невозможен (404), а UI
    при этом DAG показывает — отсюда «даг виден, но не запускается».
    """
    rows = sql(
        "SELECT d.is_paused, d.is_stale, d.last_parsed_time,"
        "       EXISTS (SELECT 1 FROM serialized_dag s WHERE s.dag_id = d.dag_id) AS serialized"
        "  FROM dag d WHERE d.dag_id = %s",
        (dag_id,))
    return rows[0] if rows else None


def dags_states(dag_ids) -> dict:
    """То же, что `dag_state`, для списка DAG-ов одним запросом: `{dag_id: {...}}`."""
    ids = sorted(set(dag_ids))
    if not ids:
        return {}
    rows = sql(
        "SELECT d.dag_id, d.is_paused, d.is_stale, d.last_parsed_time,"
        "       EXISTS (SELECT 1 FROM serialized_dag s WHERE s.dag_id = d.dag_id) AS serialized"
        "  FROM dag d WHERE d.dag_id = ANY(%s)",
        (ids,))
    return {r.pop('dag_id'): r for r in rows}


def set_paused(dag_id: str, paused: bool = True) -> int:
    """Ставит или снимает паузу. Возвращает число изменённых строк (0 — DAG-а нет)."""
    rows = sql("UPDATE dag SET is_paused = %s WHERE dag_id = %s RETURNING dag_id", (bool(paused), dag_id))
    return len(rows)


# === Задачи ===

_FINISHED = ('success', 'failed', 'skipped', 'upstream_failed', 'removed')


def set_ti_state(dag_id: str, run_id: str, task_id: str, state: str, map_index: int = -1) -> int:
    """Состояние задачи рана — то, что в Airflow 2 делал `TaskInstance.set_state`.

    Для конечных состояний проставляет `end_date` (если его нет) и `duration`, как
    `set_state`; иначе трогает только `state`. Возвращает число изменённых строк.
    """
    state = str(getattr(state, 'value', state)).lower()
    if state in _FINISHED:
        query = (
            "UPDATE task_instance"
            "   SET state = %s,"
            "       end_date = coalesce(end_date, now()),"
            "       duration = extract(epoch FROM coalesce(end_date, now()) - start_date),"
            "       updated_at = now()"
            " WHERE dag_id = %s AND run_id = %s AND task_id = %s AND map_index = %s"
            " RETURNING task_id")
    else:
        query = (
            "UPDATE task_instance SET state = %s, updated_at = now()"
            " WHERE dag_id = %s AND run_id = %s AND task_id = %s AND map_index = %s"
            " RETURNING task_id")
    return len(sql(query, (state, dag_id, run_id, task_id, map_index)))


# === Пулы ===

def pool_size(pool_name: str) -> int:
    """Слотов в пуле или 0, если пула нет. Только чтение."""
    rows = sql("SELECT slots FROM slot_pool WHERE pool = %s", (pool_name,))
    return int(rows[0]['slots'] or 0) if rows else 0


def pool_set_slots(pool_name: str, slots: int, description: str) -> bool:
    """Задаёт размер пула, создавая его при отсутствии. True — пул создан."""
    rows = sql(
        "INSERT INTO slot_pool (pool, slots, description, include_deferred)"
        " VALUES (%s, %s, %s, false)"
        " ON CONFLICT (pool) DO UPDATE SET slots = EXCLUDED.slots"
        " RETURNING (xmax = 0) AS created",
        (pool_name, slots, description))
    return bool(rows and rows[0]['created'])


def pool_create_if_missing(pool_name: str, slots: int, description: str) -> bool:
    """Создаёт пул, если его нет; существующий не трогает. True — создан сейчас."""
    rows = sql(
        "INSERT INTO slot_pool (pool, slots, description, include_deferred)"
        " VALUES (%s, %s, %s, false) ON CONFLICT (pool) DO NOTHING RETURNING pool",
        (pool_name, slots, description))
    return bool(rows)


def pool_load(pool_name: str) -> dict:
    """Задачи пула по состояниям: `{queued, running, scheduled}`."""
    rows = sql(
        "SELECT count(*) FILTER (WHERE state = 'queued')    AS queued,"
        "       count(*) FILTER (WHERE state = 'running')   AS running,"
        "       count(*) FILTER (WHERE state = 'scheduled') AS scheduled"
        "  FROM task_instance WHERE pool = %s AND state IN ('queued', 'running', 'scheduled')",
        (pool_name,))
    return {k: int(v or 0) for k, v in rows[0].items()}


# === Журнал ===

def log_event(dag_id: str, event: str, owner: str, extra: str = '') -> None:
    """Строка в журнал `log` — как делал `session.add(Log(...))`."""
    sql("INSERT INTO log (dttm, dag_id, event, owner, extra) VALUES (now(), %s, %s, %s, %s)",
        (dag_id, event[:60], owner, (extra or '')[:1000]))


def last_event(dag_id: str, events) -> dict | None:
    """Последняя строка журнала DAG-а с одним из событий: `{event, owner, dttm, extra}`."""
    rows = sql(
        "SELECT event, owner, dttm, extra FROM log WHERE dag_id = %s AND event = ANY(%s)"
        " ORDER BY dttm DESC LIMIT 1",
        (dag_id, list(events)))
    return rows[0] if rows else None


# === Заметки ===

def update_note(kind: str, key, change) -> None:
    """Заметка рана (`kind='dag'`, key=(dag_id, run_id)) или задачи (`'task'`, key=ti_id).

    `change(старый_текст) -> новый_текст | None` — None значит «не писать». Строка
    заметки создаётся пустой и перечитывается под `FOR UPDATE`: заметку дописывают
    параллельно (семь задач `ctl_loader` пишут в заметку одного рана), и без блокировки
    выигрывает последний, а строки остальных пропадают.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id=meta_conn_id())
    with closing(hook.get_conn()) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT set_config('search_path', %s, true)", (search_path(),))
                cur.execute("SELECT set_config('statement_timeout', '30s', true)")
                if kind == 'dag':
                    cur.execute("SELECT id FROM dag_run WHERE dag_id = %s AND run_id = %s", key)
                    row = cur.fetchone()
                    if not row:
                        logger.warning(f"note: рана {key} нет в метабазе")
                        return
                    table, col, ident = 'dag_run_note', 'dag_run_id', row[0]
                else:
                    table, col, ident = 'task_instance_note', 'ti_id', str(key)
                cur.execute(
                    f"INSERT INTO {table} ({col}, content, created_at, updated_at)"
                    f" VALUES (%s, NULL, now(), now()) ON CONFLICT ({col}) DO NOTHING", (ident,))
                cur.execute(f"SELECT content FROM {table} WHERE {col} = %s FOR UPDATE", (ident,))
                old = cur.fetchone()[0]
                new = change(old)
                if new is not None and new != old:
                    cur.execute(f"UPDATE {table} SET content = %s, updated_at = now() WHERE {col} = %s",
                                (new, ident))


# === Запуск DAG-а ===

def trigger_dag_run(dag_id: str, run_id: str, conf: dict | None = None) -> None:
    """Запускает DAG изнутри задачи тем же каналом, что `TriggerDagRunOperator`.

    Оператор в Airflow 3 бросает `DagRunTriggerException`, а раннер задачи отправляет
    супервизору сообщение `TriggerDagRun` → Execution API `POST /dag-runs/{dag_id}/{run_id}`
    (`sdk/execution_time/task_runner.py:_handle_trigger_dag_run`, Airflow 3.3.1). Мы шлём то же
    сообщение сами: `ctl_add_chk` одной задачей проверяет загрузку, запускает DAG и переводит
    загрузку в CTL в `WAIT-AF`, а оператор (или его маппинг) разорвал бы это на несколько
    задач с окнами «ран создан, а CTL не знает». Цена — внутренний интерфейс
    `airflow.sdk.execution_time.comms`; его держит тест `tests/test_af_meta_trigger.py` —
    при обновлении Airflow он первым скажет, что сообщение изменилось.

    `logical_date` — момент запуска, как у `trigger_dag` в Airflow 2: у ранов из CTL он
    был всегда, и шаблоны `ds`/`ts` на него опираются.

    Raises:
        DagNotFound: DAG-а нет в `serialized_dag` (Execution API: 404).
        DagRunAlreadyExists: ран с этим `run_id` уже есть (409).
    """
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse, TriggerDagRun
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    now = datetime.now(timezone.utc)
    try:
        resp = SUPERVISOR_COMMS.send(TriggerDagRun(
            dag_id=dag_id, run_id=run_id, conf=conf or {},
            logical_date=now, run_after=now, reset_dag_run=False))
    except AirflowRuntimeError as e:
        status = ((e.error.detail or {}) if isinstance(e.error.detail, dict) else {}).get('status_code')
        if status == 404:
            raise DagNotFound(f"{dag_id}: {e.error.detail}") from e
        raise
    if isinstance(resp, ErrorResponse):
        if resp.error == ErrorType.DAGRUN_ALREADY_EXISTS:
            raise DagRunAlreadyExists(f"{dag_id}: {run_id}")
        raise RuntimeError(f"Запуск {dag_id} ({run_id}) отклонён: {resp.error.value} {resp.detail}")
