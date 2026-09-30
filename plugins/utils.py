"""###🛠️ Утилиты Airflow (`plugins/utils.py`)
*2026-09-30 09:50 MSK · v1.15 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Вспомогательные функции, используемые во всех DAG'ах.

| Функция | Описание |
|---|---|
| `add_note()` | Структурированные заметки в Airflow UI (DAG/Task) |
| `add_xcom()` | Запись в XCom с обрезкой коллекций до `MAX_XCOM` элементов |
| `on_callback()` | Обработчик событий success/failure/retry |
| `pool_slots()` | Размер пула — только из сторожа подключений (`test_conn`) |
| `pool_size()` / `get_current_load()` | Размер и загрузка пула, только чтение |
| `ensure_pool()` | Создание пула, если его нет (существующий не трогает) |
| `md5_hash()` | Хеш JSON-совместимых структур |
| `readable_size()` / `readable()` | Форматирование байтов, datetime, timedelta |
| `str2timedelta()` | Парсинг строки в timedelta (`'minutes=5'`) |
| `valid_schedule()` | Проверка расписания DAG-а (cron, пресет или пусто) валидатором Airflow |
| `saved_params()` | Значения по умолчанию для формы запуска из переменной Airflow |
| `store_params()` | Запись параметров запуска в переменную (пара к `saved_params()`) |
| `safe_eval()` | Безопасное вычисление математических выражений |
| `get_conns_by_type()` / `get_conn()` | Получение соединений по типу |
| `query_to_dict()` | SQL → список словарей (Greenplum) |
| `chk_conn()` | Проверка доступности подключения Postgres / S3 / KerberosHttp |
| `update_dag_pause()` | Программная пауза/возобновление DAG'а |
| `env_stand()` | Контур из `ENV_STAND`, запасное имя — `ENVIRONMENT` |
| `report_health()` | Отчёт дага-плагина здоровья в бакет логов (`system_health/checks/<dag_id>.json`) |
"""

from airflow import settings
from airflow.models import DagModel, TaskInstance, Pool
from airflow.utils.session import provide_session, create_session
from airflow.utils.state import State
from sqlalchemy import text
from airflow.operators.python import get_current_context

from pprint import PrettyPrinter
from datetime import timedelta, datetime, timezone
from itertools import islice
import json
import hashlib
import os

from logging import getLogger, Handler
logger = getLogger("airflow.task")

MAX_NOTE_LEN = 1000
MAX_XCOM = 500


class LogCapture(Handler):
    """ Перехват логов Airflow
        _patch = [
            getLogger('airflow.task'),
            getLogger('airflow.utils.db_cleanup'),
        ]
        capture = LogCapture()
        for _l in _patch:
            _l.addHandler(capture)
        pass
        for _l in _patch:
            _l.removeHandler(capture)
    """
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


# === Утилиты ===
def env_stand() -> str:
    """Контур, на котором мы работаем: `DEV`, `IFT`, `PSI`, `PROM` или пустая строка.

    Сначала `ENV_STAND` — её читают платформенные операторы и `tools/`, — при отсутствии
    `ENVIRONMENT`: там, где выставлена только она (`tools/`), поведение не должно
    отличаться. Обе не выставлены — контур неизвестен, и вызывающий обязан считать это
    самым строгим случаем, а не стендом: переменная есть на всех контурах, включая
    тестовый (`/opt/aftest/airflow.env`).
    """
    return (os.getenv('ENV_STAND') or os.getenv('ENVIRONMENT') or '').strip().upper()

def sign(x):
    return (x > 0) - (x < 0)

def md5_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True, default=str).encode('utf-8'), usedforsecurity=False).hexdigest()

@provide_session
def pool_slots(pool_name, slots, session=None):
    """Задаёт размер пула; если пула нет — создаёт. Пишет в slot_pool.

    Звать только из сторожа подключений (CTL.<profile>.test_conn через
    chk_any_conn(manage_pool=True)): размер пулов меняет одно место. Раньше сюда же ходили
    предварительные проверки из десятка задач, и три из них сидели в самом ctl_pool — при
    сбое CTL они его обнуляли и сами больше в него не попадали.

    Прочитать размер — pool_size(): эта функция всегда пишет. Диапазон [min, max] (прежняя
    динамика «к спросу») больше не принимается: пул, растущий вслед за очередью, в пик давал
    CTL больше одновременных вызовов именно тогда, когда тому тяжелее всего.
    """
    if not isinstance(slots, int) or isinstance(slots, bool):
        raise TypeError(f"pool_slots({pool_name!r}): нужно целое число слотов, а не {slots!r}")
    slots = max(slots, 0)

    pool = session.query(Pool).filter(Pool.pool == pool_name).first()
    if pool:
        pool.slots = slots
    else:
        session.add(Pool(pool=pool_name, slots=slots, description='CTL_worker', include_deferred=False))
        logger.warning(f'Pool {pool_name} created')
    session.commit()

    if slots > 0:
        logger.info(f'Pool {pool_name} set to {slots} slots')
    else:
        logger.warning(f'Pool {pool_name} deactivated')
    return slots


@provide_session
def pool_size(pool_name, session=None) -> int:
    """Размер пула, только чтение: слотов в slot_pool или 0, если пула нет. Ничего не создаёт."""
    slots = session.query(Pool.slots).filter(Pool.pool == pool_name).scalar()
    return int(slots or 0)


TOOLS_POOL = 'tools_pool'
TOOLS_POOL_SLOTS = 16  # с запасом под tools_test_dags с его max_active_tasks=12

_ensured_pools = set()

@provide_session
def ensure_pool(pool_name, slots=TOOLS_POOL_SLOTS, description='DataLab tools', session=None):
    """Создаёт пул, если его нет; существующий не трогает.

    Не pool_slots(): та выставляет слоты на каждом вызове, а нас зовут при каждом
    парсинге DAG-файла — затирали бы значение, выставленное руками, и писали в
    метабазу впустую. Без аргумента slots она к тому же создаёт пул с нулём слотов,
    то есть намертво запирает задачи.

    Кэш на процесс: один SELECT на DagFileProcessor, а не на каждый парсинг. Цена —
    удалённый руками пул восстановится только после перезапуска процесса.

    Исключения гасим: DAG-файл не должен уходить в Broken DAG из-за метабазы.
    """
    if pool_name in _ensured_pools:
        return

    try:
        if not session.query(Pool).filter(Pool.pool == pool_name).first():
            session.add(Pool(
                pool=pool_name,
                slots=slots,
                description=description,
                include_deferred=False
            ))
            session.commit()
            logger.warning(f'Pool {pool_name} created with {slots} slots')
        _ensured_pools.add(pool_name)
    except Exception:
        logger.warning(f'Не удалось проверить пул {pool_name}', exc_info=True)

@provide_session
def get_current_load(pool_name, pool=True, session=None):
    # Считаем задачи в очереди и в работе для этого пула
    queued = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.QUEUED
    ).count()
    
    running = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.RUNNING
    ).count()

    scheduled = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.SCHEDULED
    ).count()
    
    res = dict(queued=queued, running=running, scheduled=scheduled)
    if pool: res['pool_slots'] = pool_size(pool_name, session=session)

    return res
    
def add_note(msg, context=None, level='task', add=True, title='', compact=False):

    if not context:
        context = get_current_context()
        
    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')
                    
    if type(msg) is not str:
        # Настройка для красоты:
        # indent=4 — отступ
        # width=80 — стараться не делать строку длиннее 80 символов
        # compact=False — каждое значение на новой строке
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info(f"📝 Note added to {level} {title}:\n{msg}")
    
    # Своя сессия, не create_session(): тот отдаёт scoped-сессию потока — ту же, где вызывающий
    # держит несохранённые изменения (autoflush у Airflow выключен). refresh ниже перечитывал
    # его объект и затирал их: handle_failure ставит FAILED в памяти, зовёт on_callback, и
    # callback зомби оставлял задачу running навсегда (альфа 30.09.2026, три run_prm по 5 ч).
    # lock_timeout — чтобы не ждать строку, которую держит транзакция вызывающего
    try:
        session = settings.Session.session_factory()
        try:
            if session.bind.dialect.name == 'postgresql':
                session.execute(text("SET LOCAL lock_timeout = '5s'"))
            # sorted, а не set: порядок блокировок должен быть одинаковым у всех тасков.
            # Общая строка тут одна (ран), свои task_instance у каждого свои, так что
            # взаимной блокировки не выходит, но полагаться на порядок set нельзя.
            for l in sorted(set(level.upper().split(',')))[:2]:
                new_note = msg.strip()
                # Определяем объект (DagRun или TaskInstance)
                if l == 'DAG':
                    obj = session.merge(context['dag_run'])
                else:
                    obj = session.merge(context['task_instance'])
                # Перечитываем строку под блокировкой: заметка дописывается «прочитал —
                # склеил — записал», и без FOR UPDATE двое параллельных тасков читают
                # одно и то же, а записывает последний — строка первого пропадает. У нас
                # это живой случай: семь задач ctl_loader пишут в заметку одного рана.
                # Транзакция короткая (чтение и запись одной строки), планировщик ждёт
                # миллисекунды. Не получилось перечитать под блокировкой — работаем как
                # раньше: потерянная строка заметки лучше упавшей задачи.
                try:
                    session.refresh(obj, with_for_update=True)
                except Exception as e:
                    logger.warning(f"note: строка не заблокирована ({e}), пишем без блокировки")
                    session.expire(obj)
                
                # Логика заголовка
                if title:
                    import unicodedata
                    # Если первый символ не эмодзи, то добавляем эмодзи
                    if not unicodedata.category(title[0]) == 'So':
                        title = "📝 " + title

                    new_note = f"{title}\n---\n{new_note}"

                if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                    continue
                    
                # Логика склейки заметки
                if add:
                    new_note = f"{ new_note}\n\n---\n{obj.note if obj.note else '' }"
                    
                # Лимит длины
                obj.note = new_note[:MAX_NOTE_LEN]
            
            session.commit()
        finally:
            session.close()
    except Exception as e:
        logger.warning(f"Failed to update note: {e}")

def add_xcom(key, value, context=None):
    """Кладёт значение в XCom, обрезая коллекции до MAX_XCOM элементов.

    XCom лежит в метабазе Airflow, поэтому длинные списки/словари режем — иначе
    таблица xcom пухнет, а UI на больших значениях подвисает.
    """
    if not context:
        context = get_current_context()

    ti = context['ti']
    if isinstance(value, (dict, list, tuple, set)):
        if len(value) > MAX_XCOM:
            logger.warning(f"XCom '{key}': {len(value)} элементов, обрезано до {MAX_XCOM}")
        # dict и set срезы не поддерживают — режем через islice
        if isinstance(value, dict):
            value = dict(islice(value.items(), MAX_XCOM))
        elif isinstance(value, set):
            value = list(islice(value, MAX_XCOM))
        else:
            value = value[:MAX_XCOM]
        ti.xcom_push(key=key, value=json.dumps(value, default=str))
    else:
        ti.xcom_push(key=key, value=value)

def on_callback(context, level=None): return _on_callback(context, level)

default_args = {
    'owner': 'EDP.ETL',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=timezone.utc),
    'email': ['p1080@sber.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'pool': 'default_pool',
    # 'xcom_push': True,  
    # 'execution_timeout': timedelta(minutes=15),  
    'on_failure_callback': on_callback,
    # 'on_success_callback': on_callback,
    # 'on_retry_callback': on_callback,
    # 'on_execute_callback': None,
}

def _on_callback(context, level=None):
    """ 
    Обработчик события on_callback
    """
    from airflow.utils.state import TaskInstanceState

    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag_id = ti.dag_id
    dag_state = dag_run.state
    
    if not level:
        if dag_state.lower() in ['success', 'failed'] or not context.get('task'):
            level = 'DAG'
        else:
            level = 'task'
    
    
    if level == 'DAG':
        tis = dag_run.get_task_instances()
        finished_tis = [ti for ti in tis if ti.end_date is not None ][-10:]
        last_ti = sorted(finished_tis, key=lambda x: x.end_date, reverse=True)[0] if finished_tis else []
        # failed_tis = [ti for ti in tis if ti.state == 'failed']
        # first_fail = sorted(failed_tis, key=lambda x: x.end_date)[0] if failed_tis else []
    
        task = last_ti
    else:
        task = ti
        
    task_id = task.task_id
    map_index = task.map_index
    map_ind_str = getattr(task, 'rendered_map_index', '')
    try_number = task.try_number
    state = task.state
        
    msg = context.get('exception') # or getattr(ti, 'error', None) or "No exception trace available"

    # if state == TaskInstanceState.FAILED:
    # elif state == TaskInstanceState.SUCCESS:
    # elif state == TaskInstanceState.UP_FOR_RETRY:
        
    # if not msg:
    #     xcom = ti.xcom_pull(task_ids=ti.task_id)
    #     msg = str(xcom) if xcom else ""

        
    map_str = f"   *Map Index*: ({map_index}) **{map_ind_str}**" if str(map_index) != '-1' else ""
    try_str = f"   *Try number*: **{try_number}**" if try_number > 1 else ""
    msg_str = f"```\n{str(msg)[:1000]}\n```\n" if msg else ""
    
    task_msg = f'❌ FAILED' if state.lower() == 'failed' else f'✅ SUCCESS' if state.lower() == 'success' else f'☮️ SKIPPED' if state.lower() == 'skipped' else state.upper()
    
    message = (
        f"*{datetime.now().astimezone().strftime('%d.%m.%Y %H:%M:%S %Z')}*\n\n"
    )
    
    if level == 'DAG':
        dag_msg = f'❌ FAILED' if dag_state.lower() == 'failed' else f'✅ SUCCESS'
        message += f"*DAG:* **{dag_id}**: **{dag_msg}**\n\n" 
    else:
        message += (
            f"*Task:* **{task_id}**: **{task_msg}**\n\n"
            f"{try_str}{map_str}\n\n"
            f"{msg_str}" 
        )            
    
    add_note(message, context, level, add=True)
    
    if state == TaskInstanceState.SUCCESS:
        logger.info(message)
    elif state == TaskInstanceState.FAILED:
        logger.error(message)
        if level != 'DAG':
            add_note(message, context, level='DAG', add=True)
    else:
        logger.warning(message)

def query_to_dict(gp_hook, sql, timeout=300):
    with gp_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            if timeout:
                sql = f"SET statement_timeout TO {timeout*1000}; " + sql
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def chk_conn(conn_type, conn_id, context=None, name=None, tz=None, default=False):
    """Проверяет доступность подключения `conn_id` типа `Postgres`, `S3` или `KerberosHttp`.

    Одна проверка на все даги: сторож подключений CTL (`ctl_worker.ctl_core.chk_any_conn`
    — обёртка с конфигом и пулами) и `tools_test_connections`. До 30.09.2026 у второго была
    своя копия, и правки двух копий расходились.

    name — подпись в заметке (по умолчанию `conn_id`); tz — пояс времени старта в заметке;
    default=True у Postgres — метабаза Airflow через её сессию, `conn_id` не нужен.
    Успех — заметка и результат запроса; провайдер не установлен — ☮️ и AirflowSkipException;
    сбой — заметка и AirflowFailException.
    """
    import time
    from zoneinfo import ZoneInfo
    from airflow.exceptions import AirflowFailException, AirflowSkipException

    context = context or get_current_context()
    name = name or conn_id
    ti = context['ti']
    try_number = ti.try_number
    sdt = ti.start_date.astimezone(ZoneInfo(tz) if tz else None).strftime('%Y-%m-%d %H:%M:%S %Z')
    ts = time.time()
    try:
        if conn_type == 'Postgres':
            sql = 'SELECT current_user, current_database(), inet_server_addr()'
            if default:
                with create_session() as session:
                    result = dict(session.execute(text(sql)).fetchone())
            else:
                from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
                result = query_to_dict(PostgresHook(postgres_conn_id=conn_id), sql, timeout=15)[0]

        elif conn_type == 'S3':
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
            from botocore.config import Config  # type: ignore

            extra = S3Hook.get_connection(conn_id).extra_dejson
            verify = extra.get('verify', True)
            if isinstance(verify, str):
                verify = verify.lower() == 'true'
            # config_kwargs соединения нельзя терять: явно переданный config подменяет их
            # целиком (connection_wrapper.py: `if not self.botocore_config and config_kwargs`),
            # а секрет-бэкенд кладёт туда signature_version, payload_signing_enabled и
            # request_checksum_calculation — без них шлюз отвергает запросы.
            # merge накладывает таймауты поверх, не затирая остального.
            config = Config(**extra.get('config_kwargs', {})).merge(Config(connect_timeout=15, read_timeout=15))
            result = S3Hook(aws_conn_id=conn_id, verify=verify, config=config).get_conn().list_buckets()['Buckets']

        elif conn_type == 'KerberosHttp':
            from hrp_operators.utils.kerberos_http import KerberosHttpHook  # type: ignore

            hook = KerberosHttpHook(method='GET', http_conn_id=conn_id)
            verify = hook.get_connection(conn_id).extra_dejson.get('verify', True)
            if isinstance(verify, str):
                verify = verify.lower() == 'true'
            # HttpHook.run принимает параметры requests через extra_options
            response = hook.run('/v5/api/info', headers={'Accept': 'application/json'},
                                extra_options={'timeout': 15, 'verify': verify})
            response.raise_for_status()
            result = response.json()
        else:
            result = None

        logger.info(f"🔍 {result}")
        add_note({'try': try_number, 'sdt': sdt}, context, title=f"✅ {time.time()-ts:.2f} sec chk_{name}_conn")
        return result

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"☮️ {name}: провайдер не установлен — {err}"
        add_note(msg, context, level='task', title=f"☮️ {name}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        logger.error(f"❌ {name}: {err}", exc_info=True)
        # is not None обязательно: у requests.Response __bool__ == False на 4xx/5xx,
        # то есть проверка на истинность отбросила бы ровно те ответы, ради которых всё и логируется
        response = getattr(err, 'response', None)
        if response is not None:
            logger.error(f"HTTP {getattr(response, 'status_code', '?')}: {str(getattr(response, 'text', ''))[:500]}")
        msg = f"❌ {time.time()-ts:.2f} sec chk_{name}_conn ERROR Try {try_number} {sdt}"
        add_note(err, context, level='Task,DAG', title=msg)
        raise AirflowFailException(f"{msg}: {err}") from err

def get_dict_from_ch(ch_hook, sql):
    """Выполняет SQL в ClickHouse и возвращает результат списком словарей {колонка: значение}.

    Отдельно от query_to_dict: та работает через DB-API курсор Greenplum, а ClickHouseHook
    отдаёт данные и описание колонок одним вызовом execute(with_column_types=True).
    """
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


def get_conns_by_type(conn_type='aws'):
    from airflow.configuration import get_custom_secret_backend
    from airflow.models import Connection
    
    backend = get_custom_secret_backend()
    
    if not hasattr(backend, '_local_connections'):
        return []
    local_conn: dict[str, Connection] = backend._local_connections
    
    if conn_type:
        return [id for id,conn in local_conn.items() if conn.conn_type == conn_type.lower()]
    else:
        return [id for id,conn in local_conn.items()]

def get_conn(conn_id):
    from airflow.configuration import get_custom_secret_backend
    from airflow.models import Connection
    
    backend = get_custom_secret_backend()
    
    if not hasattr(backend, '_local_connections'):
        return []
    local_conn: dict[str, Connection] = backend._local_connections
    
    connection = local_conn.get(conn_id)
    return {
                'id': conn_id,
                'host': connection.host,
                'port': connection.port,
                'schema': connection.schema,
                'description': connection.description if connection.description else "No description",
                'extra_keys': connection.extra,
            }

def readable_size(size_bytes, base=1024):
    """
    Конвертирует размер в читаемую строку. Поддерживает отрицательные числа.
    """
    if base == 1024:
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
    else:
        # Единицы без суффикса: "500", а не "500 ед" — счётчики читаются как числа
        units = ["", "тыс", "млн", "млрд", "трлн", "птлн"]

    if not size_bytes or size_bytes == 0:
        return f"0 {units[0]}".rstrip()

    import math
    
    # Запоминаем знак и работаем с модулем числа
    sign = "-" if size_bytes < 0 else ""
    size_bytes = abs(size_bytes)

    # Рассчитываем индекс юнита
    i = int(math.floor(math.log(size_bytes, base)))
    if i >= len(units): i = len(units) - 1
    if i < 0: i = 0

    size_value = round(size_bytes / (base ** i), 2)
    
    return f"{sign}{size_value} {units[i]}".rstrip()

def readable(value, base=1024, indent=4, jsn=False)->str:
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)): 
        return readable_size(value, base=base)
    elif isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M:%S %Z").strip()
    elif isinstance(value, timedelta):
        return str(value)
    else:
        if jsn:
            return json.dumps(value, indent=indent, default=str)
        else:
            return PrettyPrinter(indent=indent).pformat(value)
        

def str2timedelta(delta):
    from datetime import timedelta
    res = timedelta()
    aliases = {
        'd': 'days', 'day': 'days',
        'h': 'hours', 'hr': 'hours',
        'm': 'minutes', 'min': 'minutes',
        's': 'seconds', 'sec': 'seconds'
    }
    for s in delta.split(','):
        if '=' not in s: continue
        
        k = s.split('=')[0].strip().lower()
        v = s.split('=')[1].strip()
        
        k = aliases.get(k, k)
        
        if k not in [
            'days', 'hours', 'minutes', 'seconds', 
            'microseconds', 'milliseconds', 'weeks'
        ]: continue
        
        try: v = int(v)
        except: continue
        res += timedelta(**{k:v})
    
    return res


def valid_schedule(value) -> bool:
    """⏰ Годится ли значение как расписание DAG-а: спрашиваем сам Airflow.

    Пресеты (@daily, @quarterly) CronTriggerTimetable переводит сам через cron_presets,
    поэтому своего списка не держим — он с Airflow расходится. Отдельно проверяем только
    то, что не cron по смыслу: пустота (запуск руками), @once и @continuous.

    Годность даёт именно validate(): конструктор битое выражение глотает — ловит
    CroniterBadCronError, когда строит человекочитаемое описание, и оставляет описание
    пустым. Любая ошибка здесь означает «не годится»: вызывающий откатится на расписание
    из кода, а сорванный импорт не должен ронять парсинг DAG-а.
    """
    if value in (None, '', 'None', '@once', '@continuous'):
        return True
    try:
        from airflow.timetables.trigger import CronTriggerTimetable
        CronTriggerTimetable(str(value).strip(), timezone='Europe/Moscow').validate()
    except Exception as e:
        logger.warning(f"⚠️ расписание {value!r}: {e}")
        return False
    return True


def saved_schedule(saved, default, var_name=''):
    """⏰ Расписание DAG-а из сохранённых параметров: годное — оно, битое — из кода.

    Пара к valid_schedule(). Зовётся на парсинге в ``schedule=``: битое значение в
    переменной не должно ронять разбор файла, иначе из UI пропадёт сама форма, через которую
    расписание и чинят. Пусто или ``None`` — даг без расписания, только ручной запуск.

    Args:
        saved: Словарь из saved_params().
        default: Расписание из кода — запасной вариант и значение по умолчанию.
        var_name: Имя переменной — только для текста предупреждения.
    """
    value = saved.get('schedule', default)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {var_name}: расписание '{value}' не разобрано — беру {default}")
        return default
    return None if value in (None, '', 'None') else str(value).strip()


def store_params_task(var_name, saved, context=None, one_shot=(), flag='save_params'):
    """💾 Тело таска ``params``: store_params() и решение по его статусу.

    ``skip`` → AirflowSkipException, ``fail`` → AirflowFailException, ``ok`` → сообщение.
    Следующие таски обязаны стоять на ``trigger_rule=NONE_FAILED`` (или таск ``params`` —
    без потомков), иначе штатный пропуск утянет в skip всю цепочку.

    Args:
        var_name: Имя переменной Airflow с параметрами.
        saved: Что лежало в переменной на парсинге.
        context: Контекст таска; по умолчанию текущий.
        one_shot: Разовые галочки формы (удалить, закрыть, убить) — в переменную не пишутся.
        flag: Галочка «сохранить».
    """
    from airflow.exceptions import AirflowFailException, AirflowSkipException

    context = context or get_current_context()
    if one_shot:
        context = dict(context)
        context['params'] = {k: v for k, v in context['params'].items() if k not in one_shot}
    status, msg = store_params(var_name, saved, context, flag)
    if status == 'skip':
        raise AirflowSkipException(msg)
    if status == 'fail':
        raise AirflowFailException(msg)
    add_note(msg, context, level='task')
    return msg


def saved_params(var_name) -> dict:
    """📥 Сохранённые значения по умолчанию из переменной Airflow.

    Пара к форме запуска DAG-а: код задаёт запасной вариант, переменная — рабочий.
    Читается на парсинге, поэтому недоступная метабаза не должна ронять парсинг:
    иначе DAG пропадёт из UI ровно тогда, когда он нужнее всего. При любой ошибке
    возвращаем пусто — значения по умолчанию берутся из кода.

    Args:
        var_name: Имя переменной Airflow с JSON-словарём параметров.
    """
    from airflow.models import Variable
    try:
        return Variable.get(var_name, default_var={}, deserialize_json=True) or {}
    except Exception as e:
        logger.warning(f"⚠️ {var_name}: {e} — беру значения по умолчанию из кода")
        return {}


def store_params(var_name, saved, context=None, flag='save_params'):
    """💾 Записывает параметры запуска в переменную как новые значения по умолчанию.

    Пара к saved_params(). Зовётся из отдельного таска в начале DAG-а — тогда в переменную
    уезжает ровно то, с чем этот запуск пошёл работать. Ключ-галочка flag разовый, в
    переменную не попадает.

    Таск пропускает себя (☮️ в UI), если галочка не стоит или значения не изменились:
    лишняя запись в метабазу ни к чему. Ключ 'schedule', если он есть в форме, проверяется
    valid_schedule() — битое расписание уронило бы парсинг DAG-а, то есть убрало бы из UI
    саму форму, через которую его можно починить.

    Args:
        var_name: Имя переменной Airflow с JSON-словарём параметров.
        saved: Что лежало в переменной на парсинге — с ним сравниваем «было → стало».
        context: Контекст таска; по умолчанию берётся текущий.
        flag: Ключ-галочка «сохранить», в переменную не попадает.

    Returns:
        Кортеж ``(status, message)``: ``'ok'`` — записали, ``'skip'`` — галочка не стоит
        или значения те же, ``'fail'`` — в форме негодное расписание, переменную не
        трогали. Решение, что с этим делать, принимает таск: там же, где стоит
        ``trigger_rule`` следующего таска, — а он **обязан** быть ``NONE_FAILED``,
        если ``'skip'`` превращается в ``AirflowSkipException``, иначе штатный пропуск
        утянет в skip всю цепочку.
    """
    from airflow.models import Variable

    context = context or get_current_context()
    p = dict(context['params'])
    if not p.pop(flag, False):
        return 'skip', f'{flag}=False — параметры не сохраняем'

    if 'schedule' in p and not valid_schedule(p['schedule']):
        return 'fail', f"schedule={p['schedule']!r} — не cron и не пресет Airflow, переменную не трогаю"

    changed = {k: (saved.get(k, '—'), v) for k, v in p.items() if k not in saved or saved[k] != v}
    if not changed:
        return 'skip', f'{var_name}: значения те же — записи нет'

    # MSK круглый год UTC+3, отдельная зависимость ради этого не нужна
    ts = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    Variable.set(var_name, p, description=f"{ts} MSK · {context['dag_run'].run_id}", serialize_json=True)
    logger.info(f"💾 {var_name}: {p}")

    lines = ['| Параметр | Было | Стало |', '|----------|------|-------|'] + [
        f"| `{k}` | {was} | **{now}** |" for k, (was, now) in changed.items()
    ]
    add_note('\n'.join(lines), context=context, level='Task', title='💾 params')
    msg = f"{var_name}: {', '.join(f'{k}={now}' for k, (_, now) in changed.items())}"
    add_note(msg, context=context, level='DAG', title='💾 params')
    return 'ok', msg


@provide_session
def update_dag_pause(dag_id, paused=True, session=None):
    session.query(DagModel).filter(DagModel.dag_id == dag_id).update({"is_paused": paused})

# rand = int(safe_eval(test_mode_str) * 45*60)
def safe_eval(expr):
    """Безопасный аналог eval() для математических выражений.
    Поддерживает: +, -, *, /, **, (), числа, строки.
    Не выполняет вызовы функций или доступ к переменным.
    
    Пример: safe_eval("2 ** 3 + 1") → 9
    
    :param expr: строка с выражением
    :return: результат вычисления
    :raises ValueError: если выражение содержит запрещённые операции
    """    
    import ast
    import operator as op

    tree = ast.parse(expr.strip(), mode='eval')
    
    # Разрешённые операции
    ALLOWED_OPS = {
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow,
        ast.USub, ast.UAdd, ast.Constant, ast.Num, ast.Str
    }
    
    op_map = {
        ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul,
        ast.Div: op.truediv, ast.Pow: op.pow, ast.USub: op.neg
    }

    def _eval(node):
        if isinstance(node, ast.Constant):  # Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):     # Python < 3.8
            return node.n
        elif isinstance(node, ast.Str):     # строка
            return node.s
        elif isinstance(node, ast.BinOp) and type(node.op) in ALLOWED_OPS:
            return op_map[type(node.op)](_eval(node.left), _eval(node.right))
        elif isinstance(node, ast.UnaryOp) and type(node.op) in ALLOWED_OPS:
            return op_map[type(node.op)](_eval(node.operand))
        else:
            raise ValueError(f"Недопустимое выражение: {ast.dump(node)}")

    return _eval(tree.body)


# Опции libpq, которые имеет смысл пропускать из DB_EXTRA_1 в extra коннекта;
# всё остальное (ключи в стиле S3/ClickHouse и т.п.) psycopg2 отверг бы с
# "invalid connection option".
_LIBPQ_OPTS = {
    'connect_timeout', 'application_name', 'options', 'target_session_attrs',
    'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'gssencmode', 'krbsrvname',
}


def get_af_conn():
    """Регистрирует коннект к метабазе Airflow из Vault, возвращает conn_id.

    Нужен для VACUUM и REINDEX: требуются права владельца таблиц, которых нет у
    сессионного пользователя Airflow.
    """
    import ast
    import base64
    import json
    import os

    VAULT_PATH = '/vault/secrets/application'
    AF_ID = 'af_adm'
    # Перечитываем Vault каждый раз: процесс celery-воркера переиспользуется,
    # и закэшированный в env коннект пережил бы правку параметров подключения.
    env_key = f'AIRFLOW_CONN_{AF_ID.upper()}'

    with open(VAULT_PATH) as f:
        secrets = json.load(f)

    def _b64(s: str) -> str:
        """base64 → текст; если значение не base64 — возвращаем как есть.

        В /vault/secrets/application часть значений лежит в открытом виде, а
        b64decode без validate=True молча выбрасывает символы вне алфавита
        base64 ('-', '_', '#', скобки) и портит пароль вместо явной ошибки.
        """
        try:
            return base64.b64decode(s, validate=True).decode()
        except (ValueError, UnicodeDecodeError):
            return s

    raw_extra = _b64(secrets['DB_EXTRA_1']) if secrets.get('DB_EXTRA_1') else ''
    try:
        extra = json.loads(raw_extra) if raw_extra else {}
    except ValueError:
        extra = ast.literal_eval(raw_extra) if raw_extra else {}

    # DB_HOST_1 — failover-список "h1:port,h2:port": libpq перебирает хосты и по
    # target_session_attrs=read-write выбирает primary. В коннекте host — имена
    # через запятую без портов, port — порт первой пары (общий для всех хостов).
    conn_str = _b64(secrets['DB_HOST_1'])
    host = ','.join(h.split(':')[0] for h in conn_str.split(','))
    first = conn_str.split(',')[0]
    port = int(first.split(':')[1]) if ':' in first else 5433
    conn_json = {
        'conn_type': 'postgres',
        'host':      host,
        'port':      port,
        # Приоритет: админская учётка (DB_ADM_*), затем владелец схемы (DB_*_OWNER_1),
        # затем обычная (DB_*_1_2) — у последней прав на VACUUM/REINDEX чужих таблиц нет.
        'login':     _b64(secrets.get('DB_ADM_USER_1_1', '')) or _b64(secrets.get('DB_USER_OWNER_1', '')) or _b64(secrets.get('DB_USER_1_2', '')),
        'password':  _b64(secrets.get('DB_ADM_PASS_1_1', '')) or _b64(secrets.get('DB_PASS_OWNER_1', '')) or _b64(secrets.get('DB_PASS_1_2', '')),
        # schema в postgres-коннекте Airflow — это имя БД (dbname), а не SQL-схема;
        # 'main' — схема внутри airflowdb, и в SQL она указана явно (main.<table>).
        'schema':    _b64(secrets['DB_NAME_1']),
        # DB_EXTRA_1 поверх дефолтов, но sslmode и gssencmode задаём жёстко:
        #   sslmode=prefer — DB_EXTRA_1 приносит disable, и тогда SSL даже не
        #     пробуется; в DBeaver подключение под этой учёткой идёт с prefer;
        #   gssencmode=disable — в контейнере есть Kerberos-кэш для CTL, и libpq
        #     пробует GSS-шифрование раньше пароля, падая с "Unspecified GSS failure".
        'extra': {
            'application_name':     'airflow_db_cleanup',
            'connect_timeout':      5,
            'target_session_attrs': 'read-write',
            **{k: v for k, v in extra.items() if k in _LIBPQ_OPTS},
            'sslmode':    'prefer',
            'gssencmode': 'disable',
        },
    }
    os.environ[env_key] = json.dumps(conn_json)
    pwd = conn_json['password']
    logger.info(
        f"🔑 Коннект {AF_ID} зарегистрирован: {conn_json['login']}@{host}:{conn_json['port']}"
        f"/{conn_json['schema']} | пароль: {len(pwd)} симв."
        f" | {', '.join(f'{k}={v}' for k, v in sorted(conn_json['extra'].items()))}"
    )

    return AF_ID


#: Отчёт дага-плагина здоровья: контракт с etl-core (``mcp_health``, раздел ``plugins``
#: сводки ``get_system_health``), меняются только вместе. Плагин — даг с тегом ``health``
HEALTH_PREFIX = 'system_health/checks/'
HEALTH_SCHEMA = 1
#: Больше core не читает
HEALTH_MAX_BYTES = 256 * 1024


def report_health(checks, context=None, ttl_sec=7200):
    """Пишет отчёт проверок дага в бакет логов, откуда его читает ``get_system_health``.

    ``checks`` — ``{имя: {'status', 'summary', 'skill'?, ...}}``, статус из ``healthy``/``warn``/
    ``error``/``unknown``; остальные поля проверки уходят в ``data``. ``ttl_sec`` — через сколько
    отчёт считать просроченным: больше интервала расписания, иначе каждый поздний прогон —
    тревога. Файл один на даг и перезаписывается целиком.

    Сбой записи задачу не роняет: проверки уже в логе и заметке, а молчание плагина core
    покажет сам — «последний отчёт N назад». Возвращает ключ или None.
    """
    import re
    from airflow.configuration import conf
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    context = context or get_current_context()
    dag = context['dag']
    key = f"{HEALTH_PREFIX}{dag.dag_id}.json"
    version = re.search(r'· (v[\d.]+) ·', dag.doc_md or '')
    report = {
        'schema': HEALTH_SCHEMA,
        'source': dag.dag_id,
        'run_id': context['run_id'],
        'version': version.group(1) if version else None,
        'at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'ttl_sec': int(ttl_sec),
        'checks': {
            name: {
                'status': c.get('status', 'unknown'),
                'summary': c.get('summary', ''),
                **({'skill': c['skill']} if c.get('skill') else {}),
                'data': {k: v for k, v in c.items() if k not in ('status', 'summary', 'skill')},
            }
            for name, c in checks.items()
        },
    }
    text = json.dumps(report, ensure_ascii=False, default=str)
    if len(text.encode()) > HEALTH_MAX_BYTES:
        # Статусы и строки важнее подробностей: без data отчёт core ещё прочтёт
        for c in report['checks'].values():
            c['data'] = {'dropped': 'отчёт больше 256 КБ'}
        text = json.dumps(report, ensure_ascii=False, default=str)
    try:
        bucket = conf.get('logging', 'remote_base_log_folder').split('://', 1)[1].split('/', 1)[0]
        S3Hook(aws_conn_id=conf.get('logging', 'remote_log_conn_id')).load_string(
            text, key, bucket_name=bucket, replace=True,
            encrypt=conf.getboolean('logging', 'encrypt_s3_logs', fallback=False))
    except Exception:
        logger.warning("report_health: отчёт %s не записан", key, exc_info=True)
        return None
    logger.info("report_health: %s", key)
    return key


HEALTH_XCOM_KEY = 'health'
# Упавший проверочный таск — тоже ошибка: проверка, которая не выполнилась, не значит «здорово»
HEALTH_FAILED_STATES = ('failed', 'upstream_failed')


def push_health(checks, context=None):
    """Кладёт вердикты проверочного таска в XCom ``health``.

    ``checks`` — ``{имя: {'status', 'summary', ...}}``, формат ``report_health``. Итог подводят
    ``health_warn`` / ``health_errors`` из ``health_tasks``: сам проверочный таск находками не падает.
    """
    context = context or get_current_context()
    context['ti'].xcom_push(key=HEALTH_XCOM_KEY, value=checks)
    return checks


def _gather_health(context) -> dict:
    """Вердикты прямых upstream-тасков плюс упавшие без вердикта — как ``error``."""
    ti, task, dag_run = context['ti'], context['task'], context['dag_run']
    upstream = sorted(task.upstream_task_ids)
    if not upstream:
        return {}
    # Все предки — чтобы у upstream_failed назвать виновника, а не того, кто за ним не пошёл
    ancestors = sorted(task.get_flat_relative_ids(upstream=True))
    with create_session() as session:
        states = (session.query(TaskInstance.task_id, TaskInstance.map_index, TaskInstance.state)
                  .filter(TaskInstance.dag_id == dag_run.dag_id, TaskInstance.run_id == dag_run.run_id,
                          TaskInstance.task_id.in_(ancestors))
                  .order_by(TaskInstance.task_id, TaskInstance.map_index).all())
    rows = [r for r in states if r[0] in upstream]
    culprits = sorted({t for t, _, st in states if st == 'failed' and t not in upstream})
    checks = {}
    for task_id, map_index, state in rows:
        label = task_id if map_index < 0 else f'{task_id}[{map_index}]'
        got = ti.xcom_pull(task_ids=task_id, key=HEALTH_XCOM_KEY,
                           map_indexes=map_index if map_index >= 0 else None)
        if isinstance(got, dict) and got:
            for name, check in got.items():
                checks[name if name not in checks else f'{name}[{map_index}]'] = dict(check)
        elif state in HEALTH_FAILED_STATES:
            why = f": упал {', '.join(culprits[:5])}" if state == 'upstream_failed' and culprits else ''
            checks[label] = {'status': 'error', 'summary': f'таск {label} не выполнился ({state}){why}'}
    return checks


def health_tasks(ttl_sec, skill='tools'):
    """Два итоговых таска дага-проверки: ``health_warn`` и ``health_errors``.

    Ставятся прямыми потомками проверочных тасков, запускаются при любом их исходе
    (``ALL_DONE``). ``health_warn``: есть ``warn`` — заметка ⚠️ и успех, нет — skip.
    ``health_errors``: пишет отчёт плагина (``report_health``, единственное место записи) и
    при ``error`` падает с заметкой ❌ — ран красный, уведомление штатным колбэком; нет — skip.
    ``skill`` — навык агента этого дага, уходит в отчёт каждой проверки.
    """
    from airflow.decorators import task
    from airflow.utils.trigger_rule import TriggerRule

    def lines(checks, icon):
        return "\n\n".join(f"{icon} **{name}** — {c.get('summary', '')}" for name, c in checks.items())

    @task(task_id='health_warn', trigger_rule=TriggerRule.ALL_DONE)
    def health_warn(**context):
        """⚠️ Предупреждения проверок; нет их — skip."""
        from airflow.exceptions import AirflowSkipException

        warns = {n: c for n, c in _gather_health(context).items() if c.get('status') == 'warn'}
        if not warns:
            raise AirflowSkipException('предупреждений нет')
        add_note(lines(warns, '⚠️'), context, level='task,DAG', title=f'⚠️ предупреждений {len(warns)}')
        return {n: c.get('summary', '') for n, c in warns.items()}

    @task(task_id='health_errors', trigger_rule=TriggerRule.ALL_DONE)
    def health_errors(**context):
        """❌ Ошибки проверок и отчёт плагина здоровья; ошибок нет — skip."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        checks = _gather_health(context)
        report_health({n: {**c, 'skill': skill} for n, c in checks.items()}, context, ttl_sec=ttl_sec)
        errors = {n: c for n, c in checks.items() if c.get('status') == 'error'}
        if not errors:
            raise AirflowSkipException('ошибок нет')
        add_note(lines(errors, '❌'), context, level='task,DAG', title=f'❌ ошибок {len(errors)}')
        raise AirflowFailException('; '.join(f"{n}: {c.get('summary', '')}" for n, c in errors.items()))

    return [health_warn(), health_errors()]
