"""### 📡 DAG: Сенсор CTL
*2026-10-01 22:26 MSK · v1.10 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Каждую минуту опрашивает CTL, фильтрует загрузки в статусах `RUNNING` / `TIME-WAIT` / `EVENT-WAIT` и запускает соответствующие DAG'и через Execution API (`af_meta.trigger_dag_run`) или Asset.

Airflow 3 (v2.0): метабаза — SQL через `plugins/af_meta.py` (ORM у задачи нет), запуск DAG-а —
тем же каналом, что у `TriggerDagRunOperator`, но изнутри `ctl_add_chk`: проверка, запуск и
перевод загрузки в `WAIT-AF` остаются одной задачей (почему — в `af_meta.trigger_dag_run`).

| Задача | Описание |
|---|---|
| `ctl_add_get` | Получает список активных загрузок |
| `ctl_add_chk` | Проверяет условия и инициирует запуск |
| `ctl_add_end` | Логирует итоговое состояние |

Проверки подключений перед сбором здесь нет (до 14.09.2026 — группа `chk_conn`, шесть задач
в `pg_pool` на каждый прогон, то есть каждую минуту; падение любой, хоть S3-архива,
останавливало приём загрузок). Подключения проверяет `CTL.<profile>.test_conn` и закрывает
пулы: GP недоступен — `gp_pool` = 0, и `ctl_add_get` уходит в skip «перегрузка»; CTL
недоступен — `ctl_add_get` падает на самом вызове API.

Подробно: [ctl_worker/readme.md — ctl_sensor](../../_plugin_dag_docs/?doc=ctl_worker/readme.md#ctl_sensorpy--сенсор-событий)
"""

from airflow.sdk import DAG, Asset, AssetAlias, task
from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException


from plugins.utils import add_note, on_callback, str2timedelta, get_current_load  # type: ignore
from plugins import af_meta  # type: ignore
from plugins.af_meta import DagNotFound, DagRunAlreadyExists  # type: ignore
from ctl_worker.ctl_utils import get_config, ctl_api, ctl_obj_load, ctl_obj_save # type: ignore 
from ctl_worker.ctl_core import ctl_loading_load, ctl_chk_new, ctl_chk_expire, ctl_chk_wait, ctl_set_status, ctl_get_retry, raise_status # type: ignore

# from datetime import timedelta
import pendulum
import json
from datetime import timedelta, datetime, timezone

from logging import getLogger
logger = getLogger("airflow.task")

MAX_WFS = 50
MAX_XCOM = 500


def dag_gone_reason(dag_id: str, wf: dict | None) -> str:
    """Почему дага нет: те же условия, по которым его строит фабрика, плюс метабаза.

    Запуск берёт даг из `serialized_dag`, а UI показывает его по строке в `dag`.
    Отсюда «даг виден, но не запускается»: строка в `dag` осталась, сериализованной
    нет. Разбираем оба слоя, чтобы в заметке была причина, а не DagNotFound.
    """
    bits = []
    if not wf:
        bits.append('воркфлоу нет в ctl_workflows — фабрика его не строит')
    else:
        if wf.get('deleted'):
            bits.append('воркфлоу помечен deleted')
        if wf.get('profile') != profile:
            bits.append(f"профиль воркфлоу {wf.get('profile')}, а у нас {profile}")
    try:
        dm = af_meta.dag_state(dag_id)
        if dm is None:
            bits.append('в таблице dag записи нет')
        else:
            bits.append(f"в dag есть (is_stale={dm['is_stale']}, "
                        f"разобран {str(dm['last_parsed_time'])[:19]})")
        bits.append('сериализован' if dm and dm['serialized'] else 'в serialized_dag НЕТ — запускать нечего')
    except Exception as e:                       # диагност не должен ронять таск
        bits.append(f'метабазу опросить не вышло: {type(e).__name__}: {e}')
    return '; '.join(bits)

# Имя события для журнала метабазы: по нему отчёт tools/log_events считает, сколько раз
# сенсор упёрся в паузу. Своё, не айрфлоуское: их имена — константы планировщика.
PAUSED_EVENT = 'ctl dag paused'


def pause_reason(dag_id: str):
    """Кто и когда поставил паузу дагу — или None, если даг не на паузе.

    Происхождений четыре, и различать их важно: снимать паузу за человека нельзя, но
    сказать «воркфлоу не на расписании», когда тракт целиком выключен рубильником, —
    хуже, чем промолчать. Оператор поверит и снимет паузу.

      * поставил человек — в журнале метабазы есть `paused` (веб) или `cli_dag_pause`
        (командная строка) с именем и временем;
      * рубильник тракта — ключ `is_paused` в `ctl_config`: фабрика зовёт
        `update_dag_pause`, а тот пишет прямо в таблицу `dag`, следа в журнале нет;
      * пауза с создания: фабрика создаёт даг запаузенным, если воркфлоу не на расписании
        в CTL (`is_paused_upon_creation = not w['scheduled']`), потому что такому дагу
        `get_schedule` строит СОБСТВЕННОЕ расписание Airflow. Снять её значит заодно
        включить это расписание — не то, о чём просил CTL;
      * поставили кодом или через REST API — тоже без записи в журнале.

    Последние два по метабазе неразличимы, поэтому и не различаются в ответе: лучше
    честное «записи нет, причина одна из двух», чем уверенно названная не та.

    Сенсор паузу не снимает ни в одном случае: он называет причину и пропускает загрузку.
    """
    dm = af_meta.dag_state(dag_id)
    if not dm or not dm['is_paused']:
        return None

    # Airflow 3 пишет паузу из UI и REST событием `patch_dag` (эндпоинт PATCH /dags/{id};
    # карта имён — etl-core hrp_audit_events/af3_events.py), а не `paused` Airflow 2.
    # Направление паузы в `patch_dag` видно только по extra, поэтому его берём оттуда.
    row = af_meta.last_event(dag_id, ('paused', 'unpaused', 'cli_dag_pause', 'cli_dag_unpause', 'patch_dag'))

    # Последнее событие — снятие паузы, а даг снова на паузе: значит, её поставили без
    # журнала уже после. Такой строке верить нельзя, и она отбрасывается вместе с
    # остальными «нет записи».
    if row and (row['event'] in ('paused', 'cli_dag_pause')
                or (row['event'] == 'patch_dag' and '"is_paused": true' in (row.get('extra') or '').lower())):
        when = pendulum.instance(row['dttm']).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm')
        return f"поставил {row['owner'] or 'неизвестно кто'} {when}"

    if get_config().get('is_paused'):
        return ('рубильник тракта: ключ is_paused в ctl_config — на паузе весь профиль, '
                'снимать за человека нельзя')

    return ('записи в журнале нет: либо пауза с создания (воркфлоу не на расписании в CTL, '
            'даг ведёт расписание Airflow), либо её поставили кодом или через REST — '
            'снимать за человека нельзя')


def note_paused_event(dag_id: str, msg: str, every_min: int = 60):
    """Пишет в журнал метабазы событие о пропуске — но не чаще раза в час на даг.

    Сенсор ходит раз в минуту и статуса загрузки не меняет, так что без потолка каждая
    ждущая загрузка давала бы 1440 строк в сутки: по числу ждунов со стенда это под сорок
    тысяч записей в день ради счётчика, который считает часы, а не минуты.

    Ошибка записи гасится: это диагностика, ронять из-за неё штатный пропуск нельзя.
    """
    try:
        last = af_meta.last_event(dag_id, (PAUSED_EVENT,))
        if last and (pendulum.now('UTC') - pendulum.instance(last['dttm'])).in_minutes() < every_min:
            return False
        af_meta.log_event(dag_id, PAUSED_EVENT, 'ctl_sensor', msg)
        return True
    except Exception as e:                       # диагност не должен ронять таск
        logger.warning("не записали %s для %s: %s", PAUSED_EVENT, dag_id, e)
        return False


wfs_dict = ctl_obj_load('ctl_workflows')

profile = ctl_obj_load('ctl_profile')
profile_id = profile['id']
profile = profile['name']

default_args = {
    'owner': 'EDP.ETL',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=timezone.utc),
    'email': ['p1080@sber.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'pool': 'default_pool',
    # 'xcom_push': True,  
    # 'execution_timeout': timedelta(minutes=15),  
    'on_failure_callback': on_callback,
    # 'on_success_callback': on_callback,
    # 'on_retry_callback': on_callback,
    # 'on_execute_callback': None,
}

# with DAG('_test_dag', start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False, tags=['tools']) as dag:
#     @task
#     def test_task(): 
#         print("Hello")
#     test_task()


with DAG(f'CTL.{get_config()["profile"]}.sensor',
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=str2timedelta(get_config().get('sensor_interval','minutes=1')),
    default_args={ 
    'owner': 'EDP.ETL',
        'depends_on_past': False,
        'email': ['p1080@sber.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        "on_failure_callback": on_callback,
        "on_success_callback": None,
        "priority_weight": 999,
        'pool': 'ctl_pool',
    },
    catchup=False,
    tags=['CTL', profile, 'CTL_agent', 'sensor'],
    max_active_runs=1,
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    doc_md=__doc__,
    description='Сенсор CTL раз в минуту: запускает даги воркфлоу активных загрузок',
) as dag:

            


    # with TaskGroup(group_id='ctl_add') as ctl_add:
    # @task_group
    # def ctl_add():

    @task(pool='ctl_pool')
    def ctl_add_get(**context):
        """Сбор активных загрузок из CTL"""
        # Опрашивает API CTL и получает список активных загрузок со статусами:
        # - `RUNNING`
        # - `TIME-WAIT`
        # - `EVENT-WAIT`
        #
        # Фильтрует по:
        # - Профилю (`profile_id`)
        # - Активности (`alive == 'ACTIVE'`)
        # - Наличию `loading_id`, `wf_id`
        # - Статусу и логике предотвращения дублей
        #
        # Сохраняет промежуточные данные в `ctl_working/<lid>` (не публично).
        #
        # **Формат выхода:**
        # - Возвращает до MAX_XCOM задач в формате `params`.
        # - Пушит XCom: `lid_to_chk`, `lid_skiped`.
        #
        # **Логирование:**
        # - Таблица с `lid` и `wf_name` через `add_note`.
        # - Количество пропущенных и обработанных задач.
        #
        # **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        cl = get_current_load('gp_pool')
        if cl['pool_slots'] - cl['scheduled'] <= 1:
            msg = "🔥 System is overloaded"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)

        data = {
            'alive': '["ACTIVE"]',
            'engines': '["dummy"]',
            'profile_ids': f'[{profile_id}]',
            'status': '["RUNNING","TIME-WAIT","EVENT-WAIT"]',
        }
        try:
            ctl_api()
            tsk = ctl_loading_load(data, save=False)
        except Exception as e:
            raise AirflowFailException(f"CTL API недоступен: {e}") from e

        branch = []
        lid_skiped = {}
        lid_to_chk = {}
        wf_adding = []

        for jsn in sorted(tsk, key=lambda x: int(x['id'])):
            # lid = jsn['id']
            # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
            if jsn.get('loading_status') is not None: del jsn['loading_status']
            if jsn.get('stats') is not None: del jsn['stats']
            p = jsn.get('params', {})
            # wf_name = p.get('wfp_name') or 'unknown'
            wf_name = jsn.get('workflow',{}).get('name') or jsn.get('wf_name') 
            rep = {
                # 'alive': jsn['alive'], 
                # 'profile': jsn['profile'], 
                # 'auto': jsn.get('auto'),
                # 'type': p.get('wfp_run_type','').lower(),
                # 'start': jsn.get('start_dttm','')[:16],
                'sdt': jsn.get('status_sdt','')[:16],
                'status': jsn.get('status',''),
                'wf': wf_name.split('.')[-1],
                'log': jsn.get('status_log',''), 
            }
            
            # Проверка задания на профиль, статус и старт
            skip_add = False
            # bad response
            if jsn['alive'] != 'ACTIVE':
                rep['msg'] = 'not active'
            elif jsn['profile'] != profile:
                rep['msg'] = 'not profile'
            elif not jsn.get('id'):
                rep['msg'] = 'no id'
            elif not jsn.get('wf_id'):
                rep['msg'] = 'no wf_id'
            # not RUNNING
            elif not p.get('loading_id'):
                rep['msg'] = 'no loading_id'
            #elif jsn.get('wf_id') in wf_adding:
            elif jsn['status'] not in ['RUNNING', 'TIME-WAIT', 'EVENT-WAIT']:
                skip_add = True
                rep['msg'] = 'not add status'
            elif ( jsn['status'] == 'RUNNING' and jsn.get('status_log') ):
                skip_add = True
                rep['msg'] = 'already run'
            # add 
            elif len(branch) < MAX_WFS:
                if jsn['wf_id'] in wf_adding:
                    rep['msg'] = f"WF {wf_name} ({jsn['wf_id']}) already added"
                else:
                    skip_add = False
                    wf_adding.append(jsn['wf_id'])
                    branch.append(jsn)
                    lid_to_chk[jsn['id']] = str(rep).replace("'", "")
                    # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
            else:
                rep['msg'] = f'limit {MAX_WFS} exceeded'
            
            if skip_add and len(lid_skiped) < MAX_XCOM:
                lid_skiped[jsn['id']] = str(rep).replace("'", "")

        note = "| lid | wf_name |\n"
        note += "| ---: | :--- | \n"
        note += "\n".join([
            f"| {j.get('id')} | {j.get('wf_name','').split('.')[-1]} |" 
            for j in branch
        ])

        add_note(note, context, level='Task')
        add_note(f"⏩ lid skiped: {len(lid_skiped)}", context, level='Task,DAG')
        add_note(f"❓ lid to chk: {len(lid_to_chk)}", context, level='Task,DAG')
        # add_note(f"⚙️ lid branch: {len(branch)}", context, level='Task,DAG')
        
        ti = context['task_instance']
        ti.xcom_push(key='lid_to_chk', value=lid_to_chk)
        ti.xcom_push(key='lid_skiped', value=lid_skiped)
        
        return branch[:MAX_WFS]
    
    wfs_alias = AssetAlias(f"CTL/{profile}/wfs")
    outlets = [wfs_alias] if get_config().get('dug_run')=='dataset' else []
    @task(pool='ctl_pool', outlets=outlets,
        max_active_tis_per_dag=10, 
        map_index_template="{{ jsn['id'] }}/{{ jsn['wf_name'] }}",
        on_success_callback=None,
    )
    def ctl_add_chk(jsn, **context):
        """Проверка условий запуска и инициация workflow"""
        # Для каждой загрузки проверяет:
        # 1. Необходимость повторного запуска (`ctl_chk_new`).
        # 2. Готовность событий (`ctl_chk_expire`).
        # 3. Необходимость отложенного запуска (`wf_wait` → `ctl_chk_wait`).
        #
        # При успешной проверке:
        # - Обновляет статус в CTL на `WAIT-AF`.
        # - Запускает целевой DAG через Execution API (`af_meta.trigger_dag_run`).
        # - Или отправляет Asset при активированном режиме `dug_run`.
        #
        # **XCom:**
        # - Пушит `result` и `extra` (параметры запуска).
        # - Использует `outlet_events` для Asset-сигналов.
        #
        # **Режимы запуска:**
        # - `dug_run == 'dataset'`: сигнал через Asset.
        # - Иначе: запуск через Execution API (`af_meta.trigger_dag_run`).
        #
        # **Ссылка в логе:** добавляется ссылка на загрузку в интерфейсе CTL.
        ti = context['task_instance']
        # return ctl_chk_loading(jsn, context)

        status = jsn['status']
        log    = jsn.get('status_log') or ''
        # sdt = jsn['status_sdt']
        
        params = jsn['params']
        lid = params['loading_id']
        wid = params['wf_id']
        run_type = params.get('wfp_run_type','UNKNOWN')
        
        wf = wfs_dict.get(wid) or wfs_dict.get(str(wid))
        wf_name = (wf or {}).get('name') or jsn.get('workflow', {}).get('name') or jsn.get('wf_name')
        if not wf:
            msg = (f"⏭️ Загрузка {lid} ({wf_name}): {dag_gone_reason(f'CTL.{wf_name}', None)}")
            add_note(msg, context, level='Task,DAG', title='⏭️ НЕТ ДАГА')
            raise AirflowSkipException(msg)

        # Запаузенный даг запускать бессмысленно: запуск паузу не смотрит и создаёт
        # ран сразу в очереди, а планировщик запаузенный даг не разбирает — ран висит
        # вечно, а загрузка в CTL молча ждёт. Пропускаем, называя причину; загрузку не
        # трогаем: снимут паузу — поедет сама, как и в случае пропавшего дага.
        if (reason := pause_reason(f'CTL.{wf_name}')):
            msg = f"⏸️ Загрузка {lid} ({wf_name}): даг на паузе, {reason}"
            add_note(msg, context, level='Task,DAG', title='⏸️ ДАГ НА ПАУЗЕ')
            # Запись в журнал метабазы — чтобы такие пропуски считались числом
            # (tools/log_events.py), а не оставались строкой в логе сенсора.
            note_paused_event(f'CTL.{wf_name}', msg)
            raise AirflowSkipException(msg)
        
        af_sdt = pendulum.instance(ti.start_date).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
        params['af_sdt'] = af_sdt
        
        ctl_url = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
        msg = f"🔗 [Открыть {lid} в CTL]({ctl_url})"
        add_note(msg, context, level='Task')


        # Проверка на новый запуск и времени повторного запуска 
        # retry, is_new = ctl_chk_new(wf, params, context)
        st, res_new = ctl_chk_new(lid, wf_name, status, log, context)
        raise_status(st, res_new)
        retry, is_new = res_new
        retry = retry if retry else ctl_get_retry(wf=wf, params=params)
        
        # Проверка условия запуска на событие EVENT-WAIT
        st, res_exp = ctl_chk_expire(wf, params, context)
        raise_status(st, res_exp)
        
        # Отложенный запуск (wf_wait)
        if ( is_new and params.get('wf_wait')
            and run_type == 'EVENT-WAIT'
        ):
            st, res_wait = ctl_chk_wait(wf, params, context)
            raise_status(st, res_wait)
            
        # af_sdt = str(ti.start_date)

        # if retry:
        params['wfp_retry'] = str(retry)
        ti.xcom_push(key='extra', value=params)
        ds = f'dataset__CTL/wf/{wid}/{wf_name}'
        run_id=f'sensor__{lid}_{retry.get("try", 1)}_{af_sdt}'.replace(' ','_')
        
        # Запуск DAG
        if get_config().get('dug_run')=='dataset':
            context['outlet_events'][wfs_alias].add(Asset(ds), extra=params)
        else:
            new_dag = dict(dag_id=f'CTL.{wf_name}', run_id=run_id, conf=params, )
            try:
                af_meta.trigger_dag_run(**new_dag)
            except DagNotFound as e:
                # Дага нет в serialized_dag — запускать нечего. Загрузку в CTL не трогаем:
                # вернётся даг (следующая сборка фабрики) — поедет и она
                reason = dag_gone_reason(new_dag['dag_id'], wf)
                msg = f"⏭️ Загрузка {lid} ({wf_name}): {reason}"
                logger.error(msg, exc_info=True)
                ti.xcom_push(key='result', value={'action': '⏭️ no dag', 'id': lid,
                                                  'name': wf_name, 'msg': reason})
                add_note(msg, context, level='Task,DAG', title='⏭️ НЕТ ДАГА')
                raise AirflowSkipException(msg) from e
            except DagRunAlreadyExists as e:
                ret = {
                    "action": "❌ already",
                    "id": lid, 
                    "name": wf_name,
                    "msg": run_id
                }
                ti.xcom_push(key='result', value=ret)
                add_note(ret, context, level='Task', title='❌ ALREADY')
                raise AirflowSkipException(e)

        # Установка статуса
        status = 'RUNNING'
        log ='WAIT-AF ' + (ds if get_config().get('dug_run')=='dataset' else run_id)
        ctl_set_status(lid, status, log)
        
        ret = {
            "action": "🚀 start",
            "id": lid, 
            "name": wf_name,
            "msg": f"{status} {log}"
        }
        ti.xcom_push(key='result', value=ret)
        add_note(ret, context, level='Task', title='🚀 START')


    # default_pool: только заметки — ни CTL, ни GP не трогает (раньше сидел в pg_pool)
    @task(pool='default_pool', trigger_rule = 'none_failed')
    def ctl_add_end(res, **context):
        """Сбор и логирование результатов"""
        # Выполняется после завершения всех `ctl_add_chk`.
        # Собирает результаты из XCom и формирует итоговый отчёт.
        #
        # **Действия:**
        # - Собирает `result` из всех экземпляров `ctl_add_chk`.
        # - Преобразует в словарь `{lid: {action, name, msg}}`.
        # - Пушит в XCom под ключом `add_result`.
        # - Логирует полный список через `add_note`.
        #
        # **Режим:** `trigger_rule = 'all_done'` — выполняется всегда.
        #
        # **Назначение:** аудит, мониторинг, отладка.
        ti = context['task_instance']
        # task_ids списком: в Airflow 3 xcom_pull с одной задачей-строкой при единственной
        # найденной копии маппинга отдаёт само значение, а не список из одного
        # (sdk/execution_time/task_runner.py:xcom_pull, 3.3.1) — и цикл ниже шёл бы по
        # ключам словаря. Списком ответ всегда список, но с None на месте копий, которые
        # результата не положили (пропущенные), — в Airflow 2 их в ответе не было вовсе.
        # Оба случая поймал стенд 21.09.2026.
        wfs = [r for r in (ti.xcom_pull(key='result', task_ids=['ctl_add_chk']) or []) if r]
        wfs = {
            r['id']: {
                'action':r.get('action',''), 
                'name': r.get('name',''), 
                'msg':r.get('msg')
            } 
            for r in wfs if r['id']
        }
        # ti.xcom_push(key='add_result', value={k:str(v) for k,v in wfs.items()})
        ti.xcom_push(key='add_result', value=json.dumps(wfs, default=str))
        # add_note(wfs, context, level='Task', title='')
        
        # info = [ w['action'][:2] + w['name'] for w in wfs.values() if 'wait' not in w['action']]
        info = [ w['action'][:2] + w['name'] for w in wfs.values() ]
        add_note(info, context, level='Task', title='', compact=False)
         
        info = {}
        for w in wfs.values():
            info[w['action']] = info.get(w['action'], 0) + 1
        
        add_note(info, context, level='DAG,Task', title='')
        # return res

       
    add_get = ctl_add_get()
    add_chk = ctl_add_chk.override(doc_md=ctl_add_chk.function.__doc__).expand(jsn = add_get)
    add_end = ctl_add_end(add_chk)
        

    