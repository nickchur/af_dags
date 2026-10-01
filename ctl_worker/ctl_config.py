"""### 🔐 DAG: Конфигурация CTL
*2026-10-01 18:20 MSK · v1.15 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Сохраняет параметры системы в `Variable['ctl_config']`. Запускается вручную. Требует PIN-код (`CTL_PIN` = `AIRFLOW__CTL_PIN`).

Подробно: [ctl_worker/readme.md — ctl_config](../../_plugin_dag_docs/?doc=ctl_worker/readme.md#ctl_configpy--управление-конфигурацией)
"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, Param

from plugins.utils import on_callback, add_note, default_args, str2timedelta, get_conns_by_type, get_conn  # type: ignore
from plugins.s3_utils import s3_set_ttl, s3_create_bucket  # type: ignore
import os
import base64
import json
import pendulum

from logging import  getLogger
from datetime import datetime, timezone
logger = getLogger('airflow.task')

def get_scrt(s: str) -> str:
    """Читает секрет из /vault/secrets/application и декодирует base64 → UTF-8."""
    with open('/vault/secrets/application') as f:
        secrets = json.load(f)
    return base64.b64decode(secrets[s]).decode()


conf = Variable.get('ctl_config', default_var={}, deserialize_json=True)


def enum_default(value, allowed, fallback='off'):
    """Значение по умолчанию для списка в форме.

    В Variable может лежать что угодно — устаревшее значение или опечатка, — а `Param` с
    `enum` роняет запуск, если значение вне списка. Непонятное гасим в `fallback`: тракт
    от этого не меняется (его читают ctl_test.py и ctl_worker.py), а форма открывается.
    """
    v = str(value or '').strip().lower()
    return v if v in allowed else fallback

conns = {
    'ctl': {
        'type': 'KerberosHttp',
        'conn_id': 'ctl',
        # Фиксированный: диапазон «к спросу» в пик давал CTL больше вызовов, когда ему тяжелее
        'pool_slots': 20,
        'timeout': 30, # in seconds
        'url': "https://ctl-dev.dev.df.sbrf.ru:9080",
    },
    'gp': {
        'type': 'Postgres',
        # 'conn_id': 'alpha-adb_dev_comm-read', 
        'conn_id': conf.get('conns', {}).get('gp', {}).get('conn_id') or (
            [ c for c in get_conns_by_type('postgres') 
                  if c.startswith('alpha-') and c.endswith('-read')
            ] or ['alpha-capgp2-read'])[0], 
        'pool_slots': 20,
        'timeout': 300, # in seconds
        'schema': 's_grnplm_vd_hr_edp_srv_wf',
    },
    'pg': {
        'conn_id': 'airflowdb',
        'type': 'Postgres',
        'default': True,
    },
    # Своего S3 у CTL нет с 25.09.2026: снимки (ctl_obj_save) лежат в папке ctl/ бакета логов
    # и живут по его сроку (ctl_worker/ctl_utils.py, _ctl_s3)
    'files': {
        'type': 'S3',
        'conn_id': 's3-archive',
        'pool_slots': 20,
        "bucket": "edpetl-files",
        "ttl": 30, # days
    },
    'tfs': {
        'type': 'S3',
        'conn_id': 's3',
        "bucket": "edpetl-tfs",
        "ttl": 30, # days
    },
}

# Режимы отладочных ключей. Контуры, на которых они вообще действуют, задаются в коде
# потребителей (ctl_test.py, ctl_worker.py) и отсюда не управляются — это и есть смысл
# гейта: контур не должен переопределяться настройкой.
SIMULATOR_MODES = ['off', 'event', 'dataset', 'trigger']
TEST_MODES = ['off', 'ok', 'ok-no', 'ok-no-error']

# Кто владеет расписаниями и зависимостями (openspec/project.md, этапы переноса).
# Отсутствие ключа = mixed: выкладка кода поведения не меняет.
ORCHESTRATOR_MODES = ['ctl', 'mixed', 'af']
# Пауза при создании дага: auto разворачивается по режиму (ctl — нет, mixed — тем, чьё
# расписание строит Airflow, af — всем), yes/no перебивают режим.
PAUSE_NEW_MODES = ['auto', 'yes', 'no']

config = {
    'profile': 'HR_Data',
    'root_entity': '941010000',
    'root_category': 'p1080',
    'ue_category': "p1080.sdpue",
    "archive_category": "p1080.ARCHIVE",
    "event_expire": "time=0:00",
    # Лестница таймаутов (ctl_worker/ctl_core.py): сервер GP рвёт запрос через 4 ч 30 (правило
    # GPCC «Query Time 4,5H (GLOBAL)»), наш statement_timeout на 5 мин ниже. exe_timeout и
    # sla_time убраны 22.09.2026: первый потолком не был, второй (Airflow-SLA) не срабатывал
    'gp_server_limit': 'minutes=270',
    'gp_timeout': 'minutes=265',   # statement_timeout, если у воркфлоу нет wf_timeout
    'task_timeout': 'hours=1',     # execution_timeout задач воркера, кроме run_exe/run_tfs
    'zombie_after': 'hours=6',     # санитар: > gp_timeout + 10 мин
    'run_stale': 'hours=6',        # монитор: RUNNING → reRunned, >= zombie_after
    'lock_stale': 'hours=5',       # монитор: LOCK / LOCK-WAIT → reStarted
    'new_grace': 'minutes=60',     # монитор: моложе — загрузка «новая»
    'wait_grace': 'minutes=15',    # монитор: просрочка TIME-WAIT → reStarted
    # Потоки ue_category (исполняет не Airflow, GP соединение не рвёт — пороги свои)
    'ue_stale': 'hours=24',        # монитор UE: статус не меняется дольше → reStarted
    'ue_run_max': 'hours=24',      # монитор UE: RUNNING дольше (или wf_timeout) → reStarted
    'ue_grace': 'minutes=30',      # монитор UE: просрочка расписания/события → reStarted/Started
    # Час, а не 6 ч (30.09.2026): лог сенсора с reschedule копит все пробы окна и целиком
    # перезаливается в S3 на каждой — у events (раз в минуту) было до 346 проб в файле
    'sensor_timeout': 'hours=1',   # служебные сенсоры: events, monitor, tfs_sensor
    'sensor_retries': 10,
    'ctl_rps': 10,      # запросов к CTL в секунду из одного процесса (rate_limit); эмулятор держит тот же порог
    'ctl_limit': 1000,  #сколько записей запросить из CTL
    'ctl_days': 5, #сколько дней назад запросить из CTL
    # 'ctl_task_timeout': 'hours=+5',
    'orchestrator': 'mixed',   # кто владеет расписаниями: ctl / mixed / af
    'pause_new_dags': 'auto',  # создавать даг запаузенным: auto по режиму, либо yes/no
    'simulator': 'off',        # генератор нагрузки, ctl_test.py: off/event/dataset/trigger
    'test_mode': 'off',        # фиктивное выполнение, ctl_worker.py: off/ok/ok-no/ok-no-error
    'test_sleep': 'minutes=45',# верхняя граница ожидания вместо процедуры воркфлоу
    # Потоки с этими префиксами имени и в тестовом режиме выполняются по-настоящему: отчёты
    # строятся поверх журнала, который наполняют тестовые прогоны остальных потоков
    'test_real': ['pc1080.mail_', 'pc1080.check_'],
    'tz': 'Europe/Moscow',
    'conns': conns,
    **conf,
}

if not conf:
    ctl = get_conn('ctl')
    config['conns']['ctl']['url'] = f"{ctl.get('schema', 'https')}://{ctl.get('host')}:{ctl.get('port','9080')}"
    Variable.set('ctl_config', config, serialize_json=True, description=str(pendulum.now(config['tz']))[:19])

# Подписи полей формы: что за ключ и где о нём подробно (ctl_worker/readme.md).
# Ключ без подписи — пережиток Variable от прошлых версий, код его не читает.
CONFIG_DESCR = {
    'profile': 'Имя профиля CTL',
    'root_entity': 'Корневая сущность нашего поддерева CTL',
    'root_category': 'Корневая категория наших воркфлоу',
    'ue_category': 'Наши потоки, которые исполняет не Airflow (монитор действует, пороги ue_*)',
    'archive_category': 'Категория архивных воркфлоу: их даги не создаются',
    'event_expire': 'Срок ожидания события по умолчанию (time=Ч:ММ), если у воркфлоу нет wf_expire',
    'expire': 'Прежнее имя event_expire — читается, если event_expire не задан',
    'gp_server_limit': 'Серверный лимит запроса в Greenplum (факт, не рычаг): 4 ч 30',
    'gp_timeout': 'statement_timeout загрузки, если у воркфлоу нет wf_timeout; на 5 мин ниже серверного',
    'task_timeout': 'execution_timeout задач воркера, кроме run_exe и run_tfs',
    'zombie_after': 'Санитар: таск RUNNING с мёртвым хартбитом старше — закрыть (> gp_timeout + 10 мин)',
    'run_stale': 'Монитор: загрузка RUNNING дольше — reRunned (не меньше zombie_after)',
    'lock_stale': 'Монитор: LOCK / LOCK-WAIT дольше — reStarted',
    'new_grace': 'Монитор: загрузка моложе — «новая», не трогаем',
    'wait_grace': 'Монитор: просрочка TIME-WAIT больше — reStarted',
    'ue_stale': 'Монитор UE: статус не меняется дольше — reStarted',
    'ue_run_max': 'Монитор UE: RUNNING дольше (или wf_timeout) — reStarted',
    'ue_grace': 'Монитор UE: просрочка расписания или события больше — reStarted / Started',
    'sensor_timeout': 'Окно служебных сенсоров (events, monitor, tfs_sensor)',
    'sensor_retries': 'Ретраи служебных сенсоров',
    'ctl_rps': 'Запросов к CTL в секунду из одного процесса',
    'ctl_limit': 'Сколько записей запрашивать из CTL за раз',
    'ctl_days': 'Глубина выгрузки событий из CTL, дней',
    'test_sleep': 'Фиктивное выполнение: верхняя граница ожидания вместо процедуры',
    'test_real': 'Префиксы имён потоков, которые и в тестовом режиме выполняются по-настоящему',
    'tz': 'Часовой пояс расписаний и заметок',
    'conns': 'Подключения: ctl (url, timeout, pool_slots), gp, files. Пулы задаёт test_conn — после смены перезапустить его',
    'loader_interval': 'Интервал CTL.<профиль>.loader',
    'events_interval': 'Интервал CTL.<профиль>.events',
    'monitor_interval': 'Интервал CTL.<профиль>.monitor',
    'tfs_interval': 'Интервал CTL.<профиль>.tfs_sensor',
    'simulator_interval': 'Интервал симулятора нагрузки',
    'dagrun_timeout': 'dagrun_timeout этого дага и служебных',
}


with DAG(f'CTL.{config["profile"]}.config',
    tags=['CTL', 'CTL_agent', 'tools'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule='@once',
    catchup=False,
    default_args={ **default_args,
        "retries": 0,
        "on_failure_callback": on_callback,
        # "on_success_callback": on_callback,
    },    max_active_runs=1,
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
    dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    params={
        **{k: Param(v, description=CONFIG_DESCR.get(k)) for k, v in config.items()},
        # Списком, а не руками: значения разбираются кодом, опечатка молча выключает режим.
        # В сам config кладутся простые строки — он же уходит в Variable, а Param не
        # сериализуется.
        'orchestrator': Param(enum_default(config.get('orchestrator'), ORCHESTRATOR_MODES, 'mixed'),
                              type='string', enum=ORCHESTRATOR_MODES,
                              title='Кто владеет расписаниями (переключение перестроит все даги)',
                              description='ctl / mixed / af — этапы переноса оркестрации (openspec/project.md)'),
        'pause_new_dags': Param(enum_default(config.get('pause_new_dags'), PAUSE_NEW_MODES, 'auto'),
                                type='string', enum=PAUSE_NEW_MODES,
                                title='Новый даг создаётся запаузенным',
                              description='auto — по режиму orchestrator, yes/no перебивают режим'),
        'simulator': Param(enum_default(config.get('simulator'), SIMULATOR_MODES), type='string',
                           enum=SIMULATOR_MODES,
                           title='Симулятор нагрузки (event — только DEV)',
                              description='Генератор нагрузки ctl_test.py: off / event / dataset / trigger'),
        'test_mode': Param(enum_default(config.get('test_mode'), TEST_MODES), type='string',
                           enum=TEST_MODES,
                           title='Фиктивное выполнение (только DEV и IFT)',
                              description='ctl_worker.py: off / ok / ok-no / ok-no-error'),
        "CTL_PIN": Param('', description='PIN подтверждения; сравнивается с AIRFLOW__CTL_PIN из vault'),
    },
    doc_md=__doc__,
    description='Конфигурация CTL: параметры в Variable ctl_config, ручной запуск по PIN',
) as dag:
    
    @task
    def config_save(**context):
        """Сохранение конфигурации CTL"""
        # Выполняет:
        # - Проверку PIN-кода (`CTL_PIN == AIRFLOW__CTL_PIN`).
        # - Сохранение параметров в Airflow Variable `ctl_config`.
        # - Настройку срока хранения (TTL) для S3-бакета.
        #
        # **Логика:**
        # - Если PIN не совпадает — сохранение отменяется.
        # - После успешного сохранения — обновляется TTL в S3.
        #
        # **XCom Output:** полный словарь конфигурации.
        #
        # **Использование:**
        # - Только для администраторов.
        # - Требуется ручной запуск с подтверждением.
        
        config = context["params"]
        pin = config.pop('CTL_PIN')
        if pin == get_scrt("AIRFLOW__CTL_PIN"):
            from ctl_worker.ctl_utils import ctl_obj_save # type: ignore
            # Save config to Variable
            ctl_obj_save('ctl_config', config, var=True)
            
            msg = "✅ Configuration successfully saved to Variable 'ctl_config'"
            
            for e in os.environ:
                if e.startswith('AIRFLOW__'):
                    logger.info("⚠️{}: {}".format(e, os.getenv(e)))
        else:
            msg = "⚠️ Save skipped: 'CTL_PIN' is False}"
        
        add_note(msg, context, 'DAG,Task')
        
        
        # Бакеты и сроки хранения. Создание бакета оставлено без перехвата: без него
        # загрузкам некуда писать, и об этом надо знать сразу. А вот lifecycle-правило
        # к моменту вызова уже ничего не решает — конфигурация сохранена выше, и ронять
        # из-за него таск незачем: красный таск после успешного сохранения сбивает с толку.
        for name, bucket_key, ttl_key in (('files', 'files_bucket', 'files_ttl'),):
            s3_id = config.get('conns',{}).get(name,{}).get('conn_id')
            bucket = config.get(bucket_key)
            ttl = config.get(ttl_key)
            if not (s3_id and bucket):
                continue

            s3_create_bucket(s3_id, bucket)

            if not ttl:
                continue
            try:
                logger.info(s3_set_ttl(s3_id, bucket, days=ttl, prefix=''))
                add_note(f"⏱️ TTL {ttl}д на {bucket}", context, 'Task')
            except Exception as err:
                logger.warning("⚠️ TTL на %s не выставлен: %s", bucket, err, exc_info=True)
                add_note(f"⚠️ TTL на {bucket} не выставлен: {err}", context, 'Task')
        
        # conn = get_conn('ctl')
        # add_note(conn)
        
        return config
    
    config_save()
