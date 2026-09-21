"""### 🔌 DAG: Проверка подключений CTL
*2026-09-14 12:29 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Непрерывный сенсор (каждую минуту, `reschedule`) — проверяет доступность всех соединений из `get_config()['conns']`.
Поддерживает типы: `Postgres`, `S3`, `KerberosHttp`.

**Единственное место, где меняется размер пулов подключений** (`ctl_pool`, `gp_pool`,
`files_pool`): доступно — размер из `conns.<id>.pool_slots`, недоступно — 0. Сбой сенсор не
завершает: пул закрыт, проверка идёт дальше раз в минуту и вернёт размер, когда подключение
оживёт. Сам DAG сидит в `default_pool`, который никто не обнуляет.

**Все шесть сенсоров skipped — штатный итог рана, а не деградация.** Сенсор никогда не
возвращает «готово»: проверяет раз в минуту, пока через час его не остановит собственный
`timeout`, и `soft_fail` делает из этого skipped. Состояние задачи о здоровье подключения не
говорит: сбой виден в заметке `chk_*` (❌) и в обнулённом пуле подключения.
Красный ран — только `dagrun_timeout`: ран не уложился в отведённое время.
"""

from datetime import timedelta, datetime, timezone
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sensors.base import PokeReturnValue # type: ignore
from airflow.exceptions import AirflowSkipException

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
        # default_pool, а не пул подключения: это единственное место, где меняется размер
        # пулов (chk_any_conn(manage_pool=True)), и сидеть в пуле, который сам же обнуляет,
        # сторож не может — при сбое CTL он бы запер ctl_pool и больше туда не попал.
        # default_pool не обнуляет никто
        'pool': 'default_pool',
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
    # проверки, а не от старта рана, плюс ожидание слота в пуле перед первой и последней
    # проверкой. Замеры ниже — когда сенсоры ещё сидели в pg_pool (с 14.09 они в default_pool,
    # а шесть задач chk_conn сенсора из pg_pool убраны). Стенд 12–14.09.2026: 62 рана из 64 за 63–70 мин, первая проверка через
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
        """Одна проверка подключения. Сторож не завершается ни на успехе, ни на сбое.

        Сбой обнуляет пул подключения (это делает chk_any_conn), но сенсор НЕ падает: иначе
        soft_fail делал бы его skipped до конца рана, и пул оставался бы закрытым ещё до часа
        после того, как подключение ожило. Стенд 14.09.2026: ctl-mock остановлен в 08:46,
        chk_ctl обнулил ctl_pool и ушёл в skipped в 08:47, после запуска эмулятора пул стоял
        нулём до конца рана. Раньше пул возвращали предварительные проверки из других задач,
        теперь писатель один — значит, он обязан проверять дальше.
        """
        try:
            ret = chk_any_conn(id, data, manage_pool=True, **context)
        except AirflowSkipException:
            raise                    # провайдер не установлен — проверять нечего
        except Exception as e:
            # Пул уже обнулён, причина — в заметке ❌; следующая проверка через test_interval
            return PokeReturnValue(is_done=False, xcom_value=f"{type(e).__name__}: {str(e)[:200]}")
        return PokeReturnValue(is_done=False, xcom_value=ret)
    
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
