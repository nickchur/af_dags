"""### 🔌 DAG: Проверка подключений CTL
*2026-09-14 07:39 MSK · v1.1 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Непрерывный сенсор (каждую минуту, `reschedule`) — проверяет доступность всех соединений из `get_config()['conns']`.
Поддерживает типы: `Postgres`, `S3`, `KerberosHttp`. При сбое — экспоненциальный retry (до 1000 попыток).

**Все шесть сенсоров skipped — штатный итог рана, а не деградация.** Сенсор никогда не
возвращает «готово»: проверяет раз в минуту, пока через час его не остановит собственный
`timeout`, и `soft_fail` делает из этого skipped. Им же становится и неудачная проверка
(`AirflowFailException` при `soft_fail`), поэтому состояние задачи о здоровье подключения не
говорит: сбой виден в заметке `chk_*` (❌) и в обнулённом пуле подключения (`gp_pool` и т. п.).
Красный ран — только `dagrun_timeout`: ран не уложился в отведённое время.
"""

from datetime import timedelta, datetime, timezone
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sensors.base import PokeReturnValue # type: ignore

from plugins.utils import  on_callback, default_args, str2timedelta # type: ignore
from plugins.ctl_utils import get_config, add_note # type: ignore 
from plugins.ctl_core import chk_any_conn # type: ignore

import logging
logger = logging.getLogger("airflow.task")

profile =  get_config()['profile']
conns = get_config()['conns']

test_interval = str2timedelta(get_config().get('test_interval','minutes=1'))
timeout=timedelta(minutes=60)


with DAG(
    dag_id=f'CTL.{get_config()["profile"]}.test_conn',
    description="Проверка критически важных соединений",
    default_args={ **default_args,
        'pool': 'pg_pool',
        'max_active_runs': 1, 
        'priority_weight':1000,
        # 'sla': timedelta(minutes=5),
        # 'execution_timeout': timedelta(seconds=30), 
        'retries': 1000,
        'retry_delay': timedelta(seconds=5),
        'retry_exponential_backoff': True,  
        'max_retry_delay': test_interval,
        
        # 'on_failure_callback': on_callback,
        # 'on_success_callback': on_callback,
        # 'on_retry_callback': on_callback,
        # 'on_execute_callback': None,
    },
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval=test_interval,
    # schedule_interval=None,
    catchup=False,
    tags=['CTL', 'CTL_agent', 'tools', 'test'],
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    # sla_miss_callback = on_callback,
    # timeout (60) + 30 — запас на очередь: часовой отсчёт каждого сенсора идёт от его первой
    # проверки, а не от старта рана, плюс ожидание слота в pg_pool перед первой и последней
    # проверкой. Стенд 12–14.09.2026: 62 рана из 64 за 63–70 мин, первая проверка через
    # 1–8 мин после старта; два рана ровно по 70 мин упали по прежнему timeout + 10. На альфе
    # с 12.09 13:01 не уложился ни один (21 failed подряд) — pg_pool там делят ctl_sensor и
    # ctl_events, которые идут каждую минуту. Таймаут рана нужен только чтобы зависший ран не
    # держал max_active_runs=1; при упоре в него шедулер помечает ран failed, а недоделанные
    # задачи — skipped, то есть по сетке это неотличимо от штатного рана, кроме цвета
    dagrun_timeout= timeout + timedelta(minutes=30),
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    def chk_any(id, data=None, **context):
        try:
            ret = chk_any_conn(id, data, **context)
            return PokeReturnValue(is_done=False, xcom_value=ret)
        except Exception as e:
            # return PokeReturnValue(is_done=True, xcom_value=str(e))
            raise e
    
    # @task_group(tooltip="Проверка доступности соединений",)
    # def chk_conn():
    #     """Проверка соединений"""

    for id, data in conns.items():
        if data.get('type') not in ['Postgres', 'S3', 'KerberosHttp']:
            continue
        
        args = dict(
            task_id=f'chk_{id}', 
            doc_md=f'chk_{id} {data}',
            mode='reschedule', 
            soft_fail=True,
            poke_interval=test_interval,
            timeout=timeout,
        )
        chk_task = task.sensor(**args)(chk_any)(id=id, data=data)
    
    
    # @task(trigger_rule = 'none_failed')
    # def chk_end():
    #     pass
    
    
    # chk_conn() #>> chk_end()
