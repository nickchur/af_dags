"""### 📊 DAG: Мониторинг CTL
*2026-09-26 13:28 MSK · v1.11 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Каждые 15 минут анализирует активные загрузки и выполняет автоматические действия.

| Действие | Описание |
|---|---|
| 🔁 `reRunned` | Повторная попытка при ошибке |
| 🚫 `Aborted` | Остановка при исчерпании попыток |
| ✅ `Completed` | Успешное завершение |
| ⚠️ `reStarted` | Перезапуск зависшей задачи |
| ▶️ `Started` | Новая загрузка потока UE, у которого сорвалось расписание |
| 🚨 `SLA` | Нарушение времени выполнения |
| 🛑 `Stopped` | Остановка вручную |
| ☮️ `Skipped` | Пропуск без изменений |

Две ветки. Свои потоки (профиль контура + оркестратор `dummy`) — логика Airflow и GP.
Потоки `ue_category` на других профилях и оркестраторах — наши, но исполняет их не
Airflow: пороги `ue_stale` / `ue_run_max` / `ue_grace`, вместо `reRunned` — `reStarted`,
а поток на расписании без активной загрузки получает новую (`Started`).
"""

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DagModel, Param, Variable
from airflow.utils.session import create_session
from airflow.sensors.base import PokeReturnValue # type: ignore

from plugins.utils import add_note, on_callback, str2timedelta, get_current_load # type: ignore
from plugins.ctl_utils import get_config, gp_exe, pg_exe, ctl_obj_load, eval_delta, ctl_api # type: ignore
from plugins.ctl_core import chk_any_conn, ctl_loading_load, status_icons, ctl_wf_norm, ctl_events_mon, ctl_set_status, ctl_set_completed, ctl_wait_until, gp_timeout, cfg_delta, timeout_ladder, EXE_MARGIN, ctl_wf_owner, ctl_subtree_names  # type: ignore

import ast
import json
import re
import sys
from functools import partial
from datetime import timedelta, datetime, timezone
import pendulum

from logging import  getLogger
logger = getLogger('airflow.task')

MAX_WFS = 25

action_icons = { 
    'reRunned': '🔁', 
    'Aborted': '🚫', 
    'Completed': '✅', 
    'reStarted': '⚠️', 
    'Stopped': '🛑', 
    
    'notFound': '❓', 
    'New':'⏳' ,
    'Skipped': '☮️', 
    'SLA': '🚨',
    'Started': '▶️',
}

# Сводка SLA делится по возрасту. Нарушение вчерашнее и нарушение полугодовой давности —
# разные новости: первое разбирает дежурный, второе значит, что загрузку бросили, и её
# место в разборе расписания, а не в дежурной сводке. На альфе 17.09.2026 в одной сводке
# лежало 144 нарушения возрастом до 160 дней — заметка режется по MAX_NOTE_LEN (1000
# символов), то есть свежие нарушения в неё просто не попадали.
sla_dead = str2timedelta(get_config().get('sla_dead', 'days=30'))
sla_dead_txt = f'{sla_dead.days} д' if sla_dead.days else f'{sla_dead.seconds // 3600} ч'
# Сколько строк показывать в заметке: как у списка ждущих паузы
SLA_SHOW = 10

monitor_interval = str2timedelta(get_config().get('monitor_interval','minutes=15'))
# Пороги разбора загрузок — в конфиге, значения по умолчанию прежние (были зашиты в код).
# Лестница, с которой они обязаны сходиться, — в plugins/ctl_core.py.
new_grace = get_config().get('new_grace', 'minutes=60')     # моложе — загрузка «новая»
lock_stale = get_config().get('lock_stale', 'hours=5')      # LOCK / LOCK-WAIT → reStarted
wait_grace = cfg_delta('wait_grace', 'minutes=15')          # просрочка TIME-WAIT → reStarted
# Стандарт служебных сенсоров (22.09.2026): окно 6 ч, затем ран снимается (soft_fail → skipped)
# и следующий начинается по расписанию: лог попытки копится за всё окно, а сенсор на 12–18 ч
# в сводке здоровья выглядел как зависшая задача. Ретраи гасят разовые сбои CTL и гонку
# «executor reported success, but TI state is queued», которые иначе роняли ран целиком.
# Таймаут в AF 2.11 считается от первой попытки рана (sensors/base.py:260), так что ретраи окно
# не растягивают. Ретраи — только у сенсора: у задач после него повтор = повторное действие.
# Потоки ue_category исполняет не Airflow: соединения с GP, которое рвётся через 4 ч 30, у
# них нет, и пороги свои — сутки на зависание и работу, полчаса на просрочку запуска.
ue_stale = cfg_delta('ue_stale', 'hours=24')
ue_run_max = cfg_delta('ue_run_max', 'hours=24')
ue_grace = cfg_delta('ue_grace', 'minutes=30')
# Потоки UE на расписании, у которых нет активной загрузки: {wf_id: когда заметили}.
# Между проверками сенсор в reschedule теряет и память, и XCom — держим в Variable
UE_LOST_VAR = 'ctl_ue_lost'
sensor_timeout = str2timedelta(get_config().get('sensor_timeout', 'hours=6'))
sensor_retries = int(get_config().get('sensor_retries', 10))

def time_wait_action(lid, log, wf, sdt, now, grace, context):
    """Действие по загрузке в TIME-WAIT: reStarted, если срок старта прошёл больше `grace`.

    Время старта достаём общим разбором: наш словарь, старый текст CTL 'Start scheduled
    on …' либо расписание воркфлоу. Раньше здесь понимался ТОЛЬКО текст CTL, а всё
    остальное считалось мусором и уходило в Aborted — то есть повтор, поставленный нами
    же, монитор отменял. CTL этот текст больше не пишет, так что разбор идёт по остальным.
    """
    wait_to = ctl_wait_until(log)
    if not wait_to:
        # Лог молчит — смотрим условие запуска. Оно приходит ТОЛЬКО в полной загрузке
        # (в списке /loading/extended его нет), поэтому дотягиваем её здесь, а не для
        # каждой ожидающей. Спрашивать условие ДО расписания обязательно: при заданном
        # startCondition CTL игнорирует wf_time_sched, и посчитанное по нему время было
        # бы неправдой.
        cond = (ctl_api(f'/v4/api/loading/{lid}') or {}).get('startCondition')
        wait_to = ctl_wait_until(log, wf=wf, sdt=sdt, cond=cond)
    if not wait_to:
        # Время неизвестно НАМ, но известно CTL: он и разбудит загрузку. Пропускаем, а
        # залипшую поймает проверка SLA и покажет человеку — лучше, чем отменить молча.
        return 'Skipped'
    if pendulum.parse(wait_to, tz=get_config()['tz']) + grace <= now:
        add_note({'log': log, 'старт был назначен на': wait_to},
                 context, level='Task', title=f'reStarted {lid}')
        return 'reStarted'
    return 'Skipped'


def ue_action(lid, sts, log, sdt, now, wf, prm, context):
    """Действие монитора по загрузке потока UE (исполняет не Airflow, мы — владельцы).

    Действуем теми же вызовами CTL, что и для своих, но без предположений об Airflow и
    GP: RUNNING у чужого исполнителя — это работа, а не оборванное соединение, поэтому
    порог сутки (или `wf_timeout` воркфлоу), а вместо reRunned — reStarted: вернуть
    загрузку в RUNNING умеет только наш сенсор.
    """
    since = pendulum.parse(sdt, tz=get_config()['tz'])
    if sts == 'ERROR':
        return 'Aborted' if wf.get('faultTolerance', {}).get('abortOnFailure', False) else 'Completed'
    if sts == 'SUCCESS':
        return 'Completed'
    if sts == 'ABORTING':
        act = (log or '').split(' ')[0].strip()
        return act if act in action_icons else 'Aborted'
    if sts == 'TIME-WAIT':
        return time_wait_action(lid, log, wf, sdt, now, ue_grace, context)
    if sts == 'EVENT-WAIT':
        # ctl_events_mon сам даёт событию полчаса форы, прежде чем счесть его пропущенным
        chk = ctl_events_mon(sdt, wf, now)
        if not chk['chk']:
            add_note(chk, context, level='Task', title=f'reStarted {lid}')
            return 'reStarted'
        return 'Skipped'
    if sts == 'RUNNING':
        raw = prm.get('wf_timeout') or wf['params'].get('wf_timeout')
        limit = gp_timeout({'wf_timeout': raw})[0] if raw else ue_run_max
        return 'reStarted' if since + limit <= now else 'Skipped'
    # INIT, PREREQ, PARAM, START, LOCK, LOCK-WAIT, ERRORCHECK — стоять сутки им незачем
    return 'reStarted' if since + ue_stale <= now else 'Skipped'


with DAG(f'CTL.{get_config()["profile"]}.monitor',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'logger'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval=monitor_interval,
    catchup=False,
    default_args={
        'owner': 'EDP.ETL',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1, tzinfo=timezone.utc),
        'email': ['p1080@sber.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'pool': 'ctl_pool',
        # 'xcom_push': True,  
        # 'execution_timeout': timedelta(minutes=15),  
        'on_failure_callback': on_callback,
        # 'on_success_callback': on_callback,
        # 'on_retry_callback': on_callback,
        # 'on_execute_callback': None,
    },
    max_active_runs=1,
    is_paused_upon_creation=False,
    params={
        'zombie_dry_run': Param(
            False, type='boolean', title='Санитар: только показать',
            description='Не закрывать зависшие таски и загрузки, а только перечислить их '
                        'в заметке. Полезно первым запуском на новом контуре.',
        ),
    },
    on_failure_callback=partial(on_callback, level='DAG'),
    on_success_callback=partial(on_callback, level='DAG'),
    # страховка от зависшего рана: окно сенсора плюс час на задачи после него
    dagrun_timeout=sensor_timeout + timedelta(hours=1),
    doc_md=__doc__,
) as dag:
    
    
    @task.sensor(pool='ctl_pool',
        mode='reschedule', 
        soft_fail=True,
        poke_interval=monitor_interval,
        timeout=sensor_timeout,
        retries=sensor_retries,
    )
    def ctl_monitor(**context):
        """Sensor: опрашивает активные загрузки категории и принимает решения по каждой.

        Для каждой загрузки определяет action: reRunned / reStarted / Aborted / Completed / SLA / Skipped.
        Возвращает PokeReturnValue(is_done=True, xcom_value={lid: r}) при наличии загрузок для обработки,
        иначе is_done=False (продолжает опрос).
        """
        chk_any_conn('ctl', **context)
        ti = context['ti']

        cl = get_current_load('gp_pool')
        if cl['pool_slots'] - cl['scheduled'] <= 1:
            msg = "🔥 Sysytem is overload"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)
        
        
        now = pendulum.now(get_config()['tz'])
        # wfs = ctl_obj_load('ctl_workflows')
        
        sla_notes = {}
        # Загрузки, которым некуда ехать: даг на паузе. Сенсор их пропускает с заметкой
        # (ctl_sensor.pause_reason), но по одной строке в его логе не видно, что загрузка
        # ждёт четвёртый час. Собираем здесь и показываем возрастом, рядом с SLA.
        paused_wait = {}
        res = {}
        actions = {}
        stats = {}

        # Две ветки. Свои (профиль контура + dummy) — во всём дереве, логика Airflow и GP.
        # Потоки ue_category — любой профиль и оркестратор: исполняет не Airflow, пороги
        # свои (ue_action). До 26.09.2026 фильтр был только по dummy: чужие оркестраторы
        # монитор не видел вовсе, а dummy чужого профиля разбирал как свой.
        prf = get_config()['profile']
        profile_id = (ctl_obj_load('ctl_profile') or {}).get('id')
        ue_names = ctl_subtree_names(get_config().get('ue_category'))
        ue_active = set()      # воркфлоу UE, у которых есть активная загрузка
        ue_complete = True     # ответ CTL по UE не обрезан лимитом — можно искать пропавших

        for c, cat in ctl_obj_load('ctl_categories').items():
            is_ue = cat.get('name') in ue_names
            data = {'alive': '["ACTIVE"]', 'category_ids': f'[{c}]'}
            if not is_ue:
                data.update({'engines': '["dummy"]', 'profile_ids': f'[{profile_id}]'})
            tsk = ctl_loading_load(data, save=False)
            if is_ue and get_config().get('ctl_limit', 0) and len(tsk) >= get_config()['ctl_limit']:
                ue_complete = False
            
            for ld in sorted(tsk, key=lambda x: int(x['id'])):
                
                if ld['alive'] != 'ACTIVE': continue
                    
                lid = ld['id']
                wid = str(ld['wf_id'])
                prm = ld.get('params', {})
                wfn = ld.get('wf_name','unknown')
                # wf = wfs[wid]
                wf = ctl_api(f'/v4/api/wf/{wid}')
                wf = ctl_wf_norm(wf, None)
                # double (свой профиль + dummy внутри ue_category) исполняет Airflow — ему своя логика
                ue_ld = is_ue and ctl_wf_owner(wf, ue_names, prf) == 'ue'
                if is_ue:
                    ue_active.add(wid)

                # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
                
                sts = ld.get('status','')
                log = ld.get('status_log','')
                sdt = ld.get('status_sdt','')[:19]
                sla = prm.get('wf_interval','')
                
                stats[sts] = stats.get(sts, 0) + 1

                abortOnFailure = wf.get('faultTolerance', {}).get('abortOnFailure', False)
                scheduled = wf.get('scheduled')
                auto = ld.get('auto', False)
                running = prm.get('loading_id') is not None
                
                # t = now - datetime.strptime(sdt, "%Y-%m-%d %H:%M:%S")
                # time = f'{t}'
                t = now - pendulum.parse(sdt, tz=get_config()['tz'])
                time = (f'{t.days} d ' if t.days else '') + f'{t.hours:02}:{t.minutes:02}'
                
                action = None
                # msg = {}
                
                r = {
                    'time': time.split('.')[0],
                    'sdt': sdt[:19],
                    'SLA': sla,
                    'sch': scheduled,
                    'sts': sts, 
                    'log': True if log else False,
                    'act': None,
                    # 'msg': None,
                    'icon': None,
                    'wid': wid, 
                    'wfn': wfn,
                }
                
                # status "INIT", "TIME-WAIT", "EVENT-WAIT", "LOCK-WAIT", "PREREQ", "LOCK", "PARAM", "START", "RUNNING", "SUCCESS", "ERROR", "ERRORCHECK", "ABORTING"
                tst = pendulum.parse(eval_delta(sdt, new_grace), tz=get_config()['tz'])
                
                if tst <= now and ue_ld:
                    action = ue_action(lid, sts, log, sdt, now, wf, prm, context)
                    r['ue'] = True
                elif tst <= now and not action:
                    if sts == 'RUNNING' and not log:
                        action = 'Skipped'

                    if sts in ['TIME-WAIT', 'EVENT-WAIT'] and running:
                        action = 'Skipped'

                    elif sts in ["PREREQ", "PARAM", "START"]:
                        action = 'reStarted'

                    if sts == "ERRORCHECK":
                        action = 'reRunned'

                    elif sts == 'ERROR':
                        action = 'Aborted' if abortOnFailure else 'Completed'

                    elif sts == 'SUCCESS':
                        action = 'Completed'

                    elif sts == 'ABORTING':
                        # Недоделанное действие монитора (лог '<действие> {…}') повторяем,
                        # остальное закрываем. До 26.09.2026 ветка падала на list.strip(),
                        # а сверка шла со статусами, а не с действиями
                        action = (log or '').split(' ')[0].strip()
                        action = action if action in action_icons else 'Aborted'

                    elif sts in ['LOCK-WAIT', 'LOCK']:
                        # if datetime.strptime(eval_delta(sdt, 'hours=5'), "%Y-%m-%d %H:%M:%S") <= now:
                        if pendulum.parse(eval_delta(sdt, lock_stale), tz=get_config()['tz']) <= now:
                            action = 'reStarted'
                        else:
                            action = 'Skipped'
                            
                    elif sts == 'RUNNING' and log and not log.startswith('RUN'):
                        action = 'reRunned'

                    elif sts == 'RUNNING' and log and log.startswith('RUN'):
                        # Не раньше, чем мог кончиться сам запрос: у воркфлоу с wf_timeout
                        # 600 мин прежние 6 ч перезапускали загрузку посреди работы GP.
                        # wf_timeout — из параметров загрузки, иначе воркфлоу
                        exe_to, _ = gp_timeout({'wf_timeout': prm.get('wf_timeout') or wf['params'].get('wf_timeout')})
                        stale = max(cfg_delta('run_stale'), exe_to + EXE_MARGIN)
                        if pendulum.parse(sdt, tz=get_config()['tz']) + stale <= now:
                            action = 'reRunned'
                        else:
                            action = 'Skipped'

                    elif sts == 'TIME-WAIT' and not running:
                        action = time_wait_action(lid, log, wf, sdt, now, wait_grace, context)

                    elif sts == 'EVENT-WAIT' and not running:
                        chk = ctl_events_mon(sdt, wf, now)
                        
                        if not chk['chk']:
                            add_note(chk, context, level='Task', title=f'reStarted {lid}')
                            action = 'reStarted'
                        else:
                            action = 'Skipped'
                else:
                    action = 'New'

                if not action: 
                    action = 'notFound'
                    
                if action in ['Skipped']:
                    wf_interval = gp_exe(None, f"SELECT '{sla}'::interval") if sla else timedelta(days=1)
                    # wf_interval = timedelta(hours=6)
                    tst = pendulum.parse(sdt, tz=get_config()['tz']) + wf_interval 
                    if tst <= now:
                        action = 'SLA'


                actions[action] = actions.get(action, 0) + 1
                
                r = {**r,
                    'act': action,
                    'icon': action_icons[action],
                    # 'msg': f'{msg}',
                }
                
                # Кого считаем ждущим паузы: только тех, кому есть куда ехать прямо
                # сейчас. Таких два рода, и одного признака мало:
                #
                #   * посчитанное действие — New (запустит сенсор), reRunned и reStarted
                #     (перезапустит монитор);
                #   * RUNNING с пустым логом — CTL считает загрузку идущей, а воркер её
                #     даже не начинал. Это и есть типичный ждун паузы: сенсор пропустил
                #     запуск, лог писать некому. По действию его не поймать — монитор
                #     помечает такие Skipped и оставляет сенсору.
                #
                # Всё остальное паузы не ждёт: выполняющаяся загрузка (лог RUN),
                # доигрывающая (Completed, Aborted), спящая по таймеру (TIME-WAIT до
                # срока). Попади они в список — заметка кричала бы на нормальную работу,
                # и дежурный перестал бы её читать.
                #
                # Возраст берём тот же, что и у SLA: время с последней смены статуса
                # загрузки. «Ждёт снятия паузы 4 часа» — это про загрузку, а не про ран.
                if action in ('New', 'reRunned', 'reStarted') or (sts == 'RUNNING' and not log):
                    # Четвёртым — сам интервал: показывать надо старейших, а по строке
                    # возраста («2 d 03:14» против «12:40») их не отсортировать.
                    paused_wait[lid] = (f'CTL.{wfn}', wfn, r['time'], t)

                if action in ['Skipped', 'New']:
                    continue
                elif action in ['notFound', 'SLA']:
                    # Третьим — сам интервал: по строке возраста («160 d 04:12» против
                    # «12:40») старейших не отсортировать, а показывать надо их
                    sla_notes[lid] = (f"{r['icon']} {r['wfn']} {r['time']}", t)
                    continue
                else:
                    if len(res) < MAX_WFS:
                        res[lid] = r
                    else:
                        continue
            
        # Поток UE на расписании без активной загрузки — расписание сорвалось: по нему CTL
        # сам больше ничего не создаст. Не раньше ue_grace с момента, как заметили: между
        # концом загрузки и созданием следующей CTL нужно время. Обрезанный лимитом ответ
        # CTL не годится — недостающие загрузки выглядели бы пропавшими потоками.
        if ue_complete:
            tz = get_config()['tz']
            try:
                seen = json.loads(Variable.get(UE_LOST_VAR, default_var='{}'))
            except Exception:
                seen = {}
            lost = {}
            for wid, w in (ctl_obj_load('ctl_workflows') or {}).items():
                wid = str(wid)
                if (w.get('deleted', False) or not w.get('scheduled') or wid in ue_active
                        or ctl_wf_owner(w, ue_names, prf) != 'ue'):
                    continue
                lost[wid] = seen.get(wid) or now.to_datetime_string()
                t = now - pendulum.parse(lost[wid], tz=tz)
                if t >= ue_grace and len(res) < MAX_WFS:
                    actions['Started'] = actions.get('Started', 0) + 1
                    res[f'wf:{wid}'] = {
                        'time': (f'{t.days} d ' if t.days else '') + f'{t.hours:02}:{t.minutes:02}',
                        'sdt': lost[wid], 'SLA': '', 'sch': True, 'sts': None, 'log': False,
                        'act': 'Started', 'icon': action_icons['Started'],
                        'wid': wid, 'wfn': w.get('name', ''), 'ue': True,
                    }
                    # следующая попытка — не раньше, чем через ue_grace
                    lost[wid] = now.to_datetime_string()
            if lost != seen:
                Variable.set(UE_LOST_VAR, json.dumps(lost))

        actions = { f'{action_icons[a]}  {a}': v for a,v in actions.items()}
        add_note(actions, context, level='Task,DAG', title='Action', add=False)
        
        stats = { f'{status_icons[s]}  {s}': v for s,v in stats.items()}
        add_note(stats, context, level='Task,DAG', title='Status')
        
        # Свежие нарушения — дежурному (заметка рана), брошенные — на разбор расписания
        # (только заметка таска, иначе они вытеснят из сводки рана всё остальное).
        # Полный список обоих — в лог: заметка обрезается, а разбираться нужно по всем.
        for items, title, level in (
            ({lid: v for lid, v in sla_notes.items() if v[1] <= sla_dead},
             '🚨 SLA', 'Task,DAG'),
            ({lid: v for lid, v in sla_notes.items() if v[1] > sla_dead},
             f'🪦 Брошены дольше {sla_dead_txt}', 'Task'),
        ):
            if not items:
                continue
            logger.warning("%s: %d загрузок: %s", title, len(items),
                           {lid: v[0] for lid, v in items.items()})
            top = {lid: v[0] for lid, v in
                   sorted(items.items(), key=lambda kv: kv[1][1], reverse=True)[:SLA_SHOW]}
            if len(items) > len(top):
                top['…'] = f"и ещё {len(items) - len(top)}; полный список в логе таска"
            add_note(top, context, level=level, title=f'{title}: {len(items)}')

        # Один запрос на все увиденные даги: без него пришлось бы ходить в метабазу
        # на каждую загрузку, а их до ctl_limit за круг.
        if paused_wait:
            with create_session() as session:
                # is_active обязателен: у выпавшего из ctl_workflows воркфлоу строка в dag
                # остаётся, и без фильтра его загрузка попала бы в «ждут снятия паузы»
                # вместо диагностики пропавшего дага — а ждать ей нечего.
                paused = {r[0] for r in session.query(DagModel.dag_id).filter(
                    DagModel.dag_id.in_(sorted({v[0] for v in paused_wait.values()})),
                    DagModel.is_paused.is_(True),
                    DagModel.is_active.is_(True),
                ).all()}
            waiting = {lid: (f"⏸️ {wfn} {age}", t)
                       for lid, (dag_id, wfn, age, t) in paused_wait.items() if dag_id in paused}
            if waiting:
                logger.warning("⏸️ ждут снятия паузы: %d загрузок: %s", len(waiting),
                               {lid: v[0] for lid, v in waiting.items()})
                # В заметку — счёт и десяток старейших. Целиком список туда не влезет:
                # заметка обрезается по MAX_NOTE_LEN, и длинный список вытеснил бы Action
                # и Status, записанные в тот же тик. Полный — в логе таска.
                top = {lid: v[0] for lid, v in
                       sorted(waiting.items(), key=lambda kv: kv[1][1], reverse=True)[:10]}
                if len(waiting) > len(top):
                    top['…'] = f"и ещё {len(waiting) - len(top)}; полный список в логе таска"
                add_note(top, context, level='Task,DAG',
                         title=f'⏸️ Ждут снятия паузы: {len(waiting)}')

        chk_any_conn('ctl', **context)
        
        if res:
            return PokeReturnValue(is_done=True, xcom_value=res)
        else:
            return PokeReturnValue(is_done=False, xcom_value=stats)
    

    @task(pool='ctl_pool')
    def ctl_action(res, **context):
        """Выполняет действия над загрузками из res (XCom от ctl_monitor).

        res — dict {lid: r}, где r содержит act/wid/sch. Для каждой загрузки:
        - ABORTING статус → aborted/completed через API;
        - reRunned → RUNNING статус;
        - reStarted/Stopped → удаляет расписание, завершает, создаёт новую загрузку;
        - scheduled → ставит в расписание.
        """
        chk_any_conn('ctl', **context)
        
        active = True
        
        if not res:
            raise AirflowSkipException('Notheng to do')
        
        for lid, r in res.items():
            if r['act'] == 'Started':
                # Загрузки нет — ключ wf:<id>. Перед созданием ещё раз спрашиваем CTL:
                # решение принято по снимку воркфлоу, а загрузка могла появиться с тех пор
                wid = r['wid']
                add_note(r, context, level='Task', title=f"▶️ wf {wid} {r['wfn']}")
                try:
                    live = ctl_api('/v5/api/loading/extended', data={
                        'alive': '["ACTIVE"]', 'wfNamesLike': json.dumps([r['wfn']])}) or []
                    if any(str(x.get('wf_id')) == wid for x in live):
                        continue
                    if active: ctl_api(f"/v4/api/wf/{wid}/loading?scheduleAfterStart=true", "post", json={})
                except Exception as e:
                    logger.error(f"ctl_action Started failed for wf={wid}: {e}")
                continue

            lid = int(lid)
            wid = r['wid']
            action = r['act']
            scheduled = r['sch']

            ctl_url = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
            add_note(r, context, level='Task', title=f"🔗 {lid}({ctl_url})")

            if action in ['notFound', 'Skipped', 'New', 'SLA']:
                continue

            try:
                # Status ABORTING
                if action not in ['Completed']:
                    if active: ctl_set_status(lid, 'ABORTING', f'{action} {r}')

                # Status RUNNING
                if action in ['reRunned',]:
                    if active: ctl_set_status(lid, 'RUNNING', '')
                    continue

                # Schedule delete
                if scheduled:
                    if active: ctl_api(f'/v4/api/wf/{wid}/scheduled','delete')

                # Close Completed/Aborted
                if active: ctl_api(f"/v4/api/loading/{lid}/{'completed' if action=='Completed' else 'aborted'}", 'put')

                # Start and Schedule
                if action in ['reStarted', 'Stopped']:
                    prm = { k:str(v) for k,v in r.items() if k.startswith('wf') }
                    if active: ctl_api(f"/v4/api/wf/{wid}/loading?scheduleAfterStart={scheduled}", "post", json=prm)

                # Schedule start
                elif scheduled:
                    if active: ctl_api(f'/v4/api/wf/{wid}/scheduled','put')

            except Exception as e:
                logger.error(f"ctl_action failed for lid={lid} action={action}: {e}")

    
    @task(pool='ctl_pool')
    def ctl_zombie(**context):
        """🧟 Закрывает таски, зависшие в RUNNING с мёртвым heartbeat.

        Зачем. Умерший воркер оставляет таск в состоянии RUNNING. Планировщик объявляет
        его зомби и шлёт колбэк «пометить упавшим», но колбэк исполняется в контексте
        файла дага — а даг к тому времени мог исчезнуть (воркфлоу выпал из ctl_workflows,
        и фабрика его больше не строит). Тогда состояние не меняется никогда: те же таски
        объявляются зомби каждые десять секунд годами. На alpha так накопилось 33 штуки,
        самым старым — с апреля; они держали слоты ctl_pool, а каждое объявление дёргало
        наш on_failure_callback (за пять минут — под тысячу вызовов и 33 МБ лога).

        Что делает: помечает такие таски упавшими и закрывает осиротевшие раны. С
        загрузкой CTL за раном (`sensor__<lid>_…`) поступает по-разному, и это главное
        различие:

        * ехать некуда (нет живых задач, даг выпал из сериализации) — загрузка
          переводится в ABORTED: CTL иначе считает её активной, а идти ей действительно
          некуда;
        * ждёт снятия паузы — загрузка НЕ трогается. Снимут паузу — сенсор запустит её
          заново; закрыть её значило бы потерять загрузку из-за плановых работ. Тот же
          принцип, что у сенсора при `DagNotFound`: «вернётся даг — поедет и она».

        Порог берётся из конфига (`zombie_after`, по умолчанию 6 часов) и обязан быть
        больше потолка запроса в Greenplum (`gp_timeout` + 10 мин): лестница проверяется до
        любой уборки (`timeout_ladder`, plugins/ctl_core.py). Параметр формы
        `zombie_dry_run` показывает список, ничего не трогая.
        """
        dry = bool(context['params'].get('zombie_dry_run'))
        after = cfg_delta('zombie_after')
        secs = int(after.total_seconds())

        # Порог — единственное, что отделяет уборку от бойни: при нуле под нож идёт всё,
        # что не успело добежать. Пять минут — не «разумное значение», а граница
        # абсурда; настоящий порог обязан быть больше exe_timeout.
        if secs < 300:
            raise AirflowFailException(
                f"🔥 zombie_after = {after} — меньше пяти минут; санитар с таким порогом "
                "закрывает живые раны. Поправьте ctl_config.")
        timeout_ladder()

        # Сколько ранов закрываем за один заход. Без потолка первый боевой прогон по
        # накопленному бэклогу закрыл бы всё разом и утащил бы за собой поштучный обход
        # CTL — под statement_timeout это отказ посреди работы. Бэклог доедается за
        # несколько кругов, а «покажи» и «сделай» считают одинаково.
        LIMIT = 500

        # Критерий один на выборку и на правку: таск в RUNNING, а его job либо не
        # зарегистрирован, либо не бился дольше порога. Живой таск со свежим heartbeat
        # сюда не попадает ни при каких условиях — это главное свойство запроса.
        where = (
            "ti.state = 'running' AND ("
            "  ti.job_id IS NULL"
            "  OR j.id IS NULL"
            "  OR j.state <> 'running'"
            f"  OR j.latest_heartbeat < now() - interval '{secs} seconds')"
        )

        stuck = pg_exe(
            "SELECT ti.dag_id, ti.task_id, ti.run_id, ti.map_index, ti.start_date,"
            "       j.state AS job_state, j.latest_heartbeat"
            "  FROM task_instance ti"
            "  LEFT JOIN job j ON j.id = ti.job_id"
            f" WHERE {where}"
            " ORDER BY ti.start_date LIMIT 500"
        )

        note = {
            f"{r['dag_id']}.{r['task_id']}": {
                'run_id': r['run_id'],
                'start': str(r['start_date'])[:19],
                'heartbeat': str(r['latest_heartbeat'])[:19] if r['latest_heartbeat'] else 'нет job',
            }
            for r in stuck
        }

        # Раны, которые планировщик разбирает вхолостую на каждом круге. Считаются
        # ОТДЕЛЬНО от тасков: ран висит и после того, как его таски добиты — хоть нами,
        # хоть штатным зомби-килом. Три случая, все старше порога, но исходы у них
        # РАЗНЫЕ, поэтому и запроса два.
        #
        # «Ехать некуда» — ран закрывается, загрузка за ним переводится в ABORTED:
        #
        #   1. RUNNING, в котором не осталось ни одной живой задачи — двигаться ему некуда;
        #   2. RUNNING или QUEUED у дага, которого нет в serialized_dag: воркфлоу выпал
        #      из ctl_workflows, фабрика его больше не строит, и планировщик на каждом
        #      круге пишет «DAG ... not found in serialized_dag». На alpha один такой
        #      призрак давал 60 строк ошибок в секунду.
        #
        # «Поедет, но позже» — ран закрывается, ЗАГРУЗКА НЕ ТРОГАЕТСЯ:
        #
        #   3. QUEUED у запаузенного дага: trigger_dag состояние паузы не смотрит и
        #      создаёт ран сразу в очереди, а планировщик запаузенный даг не разбирает.
        #      Такой ран не поедет, пока паузу не снимут, — и копится в очереди.
        #
        # Разница принципиальная. Пауза — это плановые работы, а не смерть: снимут её —
        # сенсор запустит загрузку заново (он и делает это, пока у неё пустой status_log).
        # Закрывать загрузку из-за ночного окна работ значило бы терять её на ровном
        # месте. Ран же закрыть надо: он всё равно не поедет — сенсор создаст новый.
        #
        # Порядок запросов важен: случай 2 забирает раны дагов, пропавших из сериализации,
        # включая запаузенные, — после него они уже 'failed' и во второй запрос не попадут.
        #
        # Живые состояния перечислены явно, и up_for_reschedule среди них обязателен:
        # сенсоры в режиме reschedule (tfs_wait ждёт файл до суток) между опросами живут
        # именно в нём. Без него санитар убивал бы их на шестом часе ожидания — включая
        # собственный ран монитора.
        LIVE_TASK = "('running','queued','scheduled','up_for_retry','deferred'," \
                    "'up_for_reschedule','restarting')"
        AGE = ("coalesce(dr.start_date, dr.queued_at, dr.execution_date)"
               f" < now() - interval '{secs} seconds'")

        PAUSED = "EXISTS (SELECT 1 FROM dag d WHERE d.dag_id = dr.dag_id AND d.is_paused)"
        # Запаузенный даг из «нет живых задач» исключён: у его рана следующая задача так и
        # остаётся без состояния (планировщик паузу не разбирает), и раньше такой ран уходил
        # сюда — вместе с загрузкой в ABORTED. Пауза — не смерть: его забирает paused_where.
        dead_where = (
            f"dr.state IN ('running','queued') AND {AGE}"
            "   AND ("
            f"        (dr.state = 'running' AND NOT {PAUSED}"
            "         AND NOT EXISTS (SELECT 1 FROM task_instance ti"
            "                          WHERE ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id"
            f"                            AND ti.state IN {LIVE_TASK}))"
            "     OR NOT EXISTS (SELECT 1 FROM serialized_dag sd WHERE sd.dag_id = dr.dag_id)"
            "       )"
        )
        # Ран запаузенного дага: в очереди — по возрасту; в работе — если задач в running
        # нет и ничего не заканчивалось дольше порога. dagrun_timeout такому рану не
        # поможет: планировщик AF 2.11.2 запаузенные даги не разбирает вовсе
        # (models/dagrun.py:412), а таймаут проверяет там же (scheduler_job_runner.py:1665).
        # Задачу в running не трогаем при любом wf_timeout: запрос в GP доработает, и ран
        # закроется на следующем круге. Отсчёт — от последнего движения, а не от старта
        # рана: иначе ран, шедший пять часов и запаузенный сейчас, закрылся бы через час.
        paused_where = (
            f"{PAUSED} AND ("
            f"    (dr.state = 'queued' AND {AGE})"
            "  OR (dr.state = 'running'"
            "      AND NOT EXISTS (SELECT 1 FROM task_instance ti"
            "                       WHERE ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id"
            "                         AND ti.state = 'running')"
            "      AND coalesce((SELECT max(ti.end_date) FROM task_instance ti"
            "                     WHERE ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id),"
            "                   dr.start_date, dr.queued_at)"
            f"          < now() - interval '{secs} seconds'))"
        )

        def orphan_sql(action, where):
            """Отбор ранов: один и тот же для «покажи» и «сделай» — порядок и потолок.

            Потолок вешается на подзапрос с id, а не на сам UPDATE: PostgreSQL не знает
            LIMIT в UPDATE, а без него первый прогон по бэклогу закрывает всё разом.
            """
            return (
                f"{action} WHERE dr.id IN ("
                f"  SELECT dr.id FROM dag_run dr WHERE {where}"
                "   ORDER BY coalesce(dr.start_date, dr.queued_at, dr.execution_date)"
                f"  LIMIT {LIMIT})"
            )

        def lids_of(rows):
            """Номера загрузок CTL из имён ранов, по возрастанию и без повторов.

            Идентификатор лежит в имени рана, который создаёт сенсор:
            `sensor__<lid>_<попытка>_<дата>`. Раны других происхождений (ручные,
            расписания) имени не подходят и в ответ не попадают.
            """
            return sorted({int(m.group(1)) for r in rows
                           if (m := re.match(r'sensor__(\d+)_', str(r['run_id'])))})

        if dry:
            # Отбор тот же, что в боевой ветке ниже, иначе «покажи» и «сделай» показывали
            # бы разное. Полное число — отдельным счётом: потолок скрывает размер бэклога,
            # а знать его нужно именно до первого боевого прогона.
            cols = "SELECT dr.dag_id, dr.run_id FROM dag_run dr"
            dead = pg_exe(orphan_sql(cols, dead_where))
            paused = pg_exe(orphan_sql(cols, paused_where))
            total = pg_exe(f"SELECT (SELECT count(*) FROM dag_run dr WHERE {dead_where}) AS dead,"
                           f"       (SELECT count(*) FROM dag_run dr WHERE {paused_where}) AS paused")[0]
            add_note({'🧟 Санитар (только показать)': note or 'зависших тасков нет',
                      f"осиротевшие раны (найдено {total['dead']}, потолок {LIMIT})":
                          [f"{r['dag_id']} / {r['run_id']}" for r in dead] or 'нет',
                      f"⏸️ раны на паузе (найдено {total['paused']}, потолок {LIMIT})":
                          [f"{r['dag_id']} / {r['run_id']}" for r in paused] or 'нет',
                      'загрузки под ABORTED': lids_of(stuck + dead),
                      'загрузки, которые не трогаем (ждут снятия паузы)': lids_of(paused)},
                     context, level='Task,DAG')
            return {'stuck': len(stuck), 'runs': len(dead), 'paused': len(paused),
                    'loadings': lids_of(stuck + dead), 'dry_run': True}

        # RETURNING, а не «обновили и надеемся»: в отчёт и в закрытие загрузок идёт то,
        # что база действительно поменяла, а не то, что показала выборка секунду назад.
        failed = []
        if stuck:
            failed = pg_exe(
                "UPDATE task_instance ti SET state = 'failed', end_date = now()"
                "  FROM job j WHERE j.id = ti.job_id AND " + where +
                " RETURNING ti.dag_id, ti.task_id, ti.run_id"
            )
            # Таски без job вообще (ti.job_id пуст) отдельным запросом: JOIN их не поймает.
            # Порог здесь тот же, что и везде: между «таск встал в running» и «job
            # записался» есть окно, и попадать в него санитару незачем.
            failed += pg_exe(
                "UPDATE task_instance SET state = 'failed', end_date = now()"
                " WHERE state = 'running' AND job_id IS NULL"
                f"   AND start_date < now() - interval '{secs} seconds'"
                " RETURNING dag_id, task_id, run_id"
            )

        upd = "UPDATE dag_run dr SET state = 'failed', end_date = now()"
        ret = " RETURNING dr.dag_id, dr.run_id"
        # Порядок: сперва «ехать некуда» — иначе запаузенный ран пропавшего дага попал бы
        # во второй запрос и загрузка осталась бы висеть активной.
        runs = pg_exe(orphan_sql(upd, dead_where) + ret)
        # Ран на паузе в работе закрываем так же, как сам Airflow по dagrun_timeout: ран —
        # failed, незаконченные задачи — skipped (running среди них нет по отбору). was —
        # прежнее состояние рана, для заметки: очередь и работа — разные истории.
        paused_runs = pg_exe(
            "WITH closed AS ("
            "  UPDATE dag_run dr SET state = 'failed', end_date = now()"
            "    FROM (SELECT dr.id, dr.state AS was FROM dag_run dr"
            f"          WHERE {paused_where}"
            "          ORDER BY coalesce(dr.start_date, dr.queued_at, dr.execution_date)"
            f"         LIMIT {LIMIT}) o"
            "   WHERE dr.id = o.id"
            "   RETURNING dr.dag_id, dr.run_id, o.was"
            "), skipped AS ("
            "  UPDATE task_instance ti SET state = 'skipped', end_date = now()"
            "    FROM closed c"
            "   WHERE ti.dag_id = c.dag_id AND ti.run_id = c.run_id AND c.was = 'running'"
            "     AND (ti.state IS NULL OR ti.state IN ('scheduled','queued','up_for_retry',"
            "                                           'up_for_reschedule','deferred','restarting'))"
            "   RETURNING 1"
            ")"
            " SELECT c.dag_id, c.run_id, c.was, (SELECT count(*) FROM skipped) AS ti_skipped FROM closed c"
        )

        # Загрузки берутся и с тасков, и с ранов: у рана-призрака живых тасков нет
        # вовсе, а загрузка за ним всё равно числится активной. Раны с паузы сюда НЕ
        # входят — за ними стоят живые загрузки, которые поедут после снятия паузы.
        reasons = {lid: f'Zombie: таск не бился дольше {after}, закрыт санитаром'
                   for lid in lids_of(failed)}
        for lid in lids_of(runs):
            # Своя причина: у осиротевшего рана таск мог не запускаться вовсе, и «таск не
            # бился» увело бы разбор по ложному следу.
            reasons.setdefault(lid, f'Zombie: ран осиротел дольше {after} '
                                    '(нет живых задач или даг выпал из сериализации), '
                                    'закрыт санитаром')

        closed, skipped = [], []
        for lid, reason in reasons.items():
            try:
                ld = ctl_api(f"/v4/api/loading/{lid}")
                if (ld or {}).get('alive') != 'ACTIVE':
                    skipped.append(lid)
                    continue
                ctl_set_status(lid, 'ERROR', reason)
                ctl_set_completed(lid, completed=False)
                closed.append(lid)
            except Exception as e:                       # одна загрузка не должна ронять уборку
                logger.error(f"ctl_zombie: не закрыл загрузку {lid}: {e}")

        if not (failed or runs or paused_runs):
            add_note('🧟 Зависших тасков и осиротевших ранов нет', context, level='Task')
            return {'stuck': 0, 'runs': 0, 'paused': 0}

        add_note({'🧟 Закрыто зависших тасков': len(failed), 'таски': note,
                  'закрыто ранов': len(runs), 'загрузки ABORTED': closed,
                  'уже закрыты': skipped,
                  '⏸️ закрыто ранов на паузе': {
                      'в очереди': sum(r['was'] == 'queued' for r in paused_runs),
                      'в работе': sum(r['was'] == 'running' for r in paused_runs),
                      'задач → skipped': paused_runs[0]['ti_skipped'] if paused_runs else 0},
                  'их загрузки не тронуты': lids_of(paused_runs)},
                 context, level='Task,DAG')
        return {'stuck': len(failed), 'runs': len(runs), 'paused': len(paused_runs),
                'closed': closed, 'skipped': skipped}


    ctl_action(res = ctl_monitor())
    ctl_zombie()

