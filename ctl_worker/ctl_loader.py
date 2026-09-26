"""### 📥 DAG: Загрузчик метаданных CTL
*2026-09-26 13:28 MSK · v1.5 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Раз в `loader_interval` (по умолчанию 5 минут) выгружает данные из CTL и сохраняет в Airflow Variables + S3 (папка `ctl/` бакета логов).

| Объект | Описание |
|---|---|
| `ctl_profile` | Профиль CTL |
| `ctl_categories` | Дерево категорий |
| `ctl_workflows` | Workflow'ы с параметрами |
| `ctl_entities` / `ctl_enames` | Иерархия сущностей + имена |
| `ctl_events` / `ctl_ue_events` / `ctl_entity_events` | События за последние N дней; висячие ссылки (нет сущности или профиля в CTL) отсекаются |

Данные доступны через `ctl_obj_load()`.
"""

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowRescheduleException


from plugins.utils import add_note, on_callback, readable_size, str2timedelta, md5_hash # type: ignore
from plugins.ctl_utils import get_config,  ctl_obj_save, ctl_obj_load, ctl_api # type: ignore
from plugins.ctl_core import ctl_loading_load, ctl_wf_norm, chk_any_conn, ctl_wf_owner, ctl_subtree_names, AF_ENGINE # type: ignore


from functools import partial
import pendulum
from datetime import timedelta, datetime, timezone

from logging import  getLogger
logger = getLogger('airflow.task')


def load_obj_save(obj, data, var=False, skip=False, **context):
    """Сохранить объект; вернуть, изменился ли он."""
    # Сохраняем в S3
    if ctl_obj_save(obj, data, var=var):
        add_note(f"🔍 {obj}: {len(data)} / {readable_size(len(str(data)))}", context, level='DAG,Task')
        return True
    msg = f'⚠️ {obj} не изменились'
    add_note(msg, context, level='DAG,Task')
    if skip:
        raise AirflowSkipException(msg)
    return False


def wf_coverage(wfs, categories, profile):
    """Потоки дерева, за которые никто не отвечает (`orphan`) или отвечают двое (`double`).

    Каждый наш поток должен быть либо Airflow (свой профиль + dummy), либо в `ue_category`
    (исполняет другой, следит монитор). Ничей поток теряется молча: дага нет, монитор не
    смотрит. Удалённые и архив (`archive_category`) не в счёт — их не исполняет никто
    намеренно. Зато архивный поток на расписании — аномалия: в архиве старые потоки, и
    запускаться им незачем (`archive_live`).
    Возвращает {'orphan': {wf_id: описание}, 'double': {...}, 'archive_live': {...}}.
    """
    ue = ctl_subtree_names(get_config().get('ue_category'), categories)
    archive = ctl_subtree_names(get_config().get('archive_category', 'p1080.ARCHIVE'), categories)
    out = {'orphan': {}, 'double': {}, 'archive_live': {}}
    for wid, w in wfs.items():
        if w.get('deleted', False):
            continue
        kind = ('archive_live' if w.get('scheduled') else None) if w.get('category') in archive \
            else ctl_wf_owner(w, ue, profile)
        if kind in out:
            out[kind][wid] = f"{w.get('name')} · {w.get('profile')}/{w.get('engine')} · {w.get('category')}"
    return out


# Больше этой доли висячих — не верим ответу CTL, а не отсекаем полсписка событий.
DANGLING_MAX_SHARE = 0.25


def ctl_profile_names():
    """Имена профилей CTL (`GET /v4/api/profile`) или None, если списку верить нельзя.

    Регистр имеет значение: на alpha 14.09.2026 CTL отвечал «Profile with name ARNSDPCC360
    does not exist», хотя живые ссылки того же профиля записаны `arnsdpcc360`. Список без
    собственного профиля — не список профилей (другой формат ответа, страница), и отсева
    по нему не будет.
    """
    try:
        data = ctl_api('/v4/api/profile')
    except Exception as e:
        logger.warning(f"⚠️ Список профилей CTL не получен: {e}")
        return None
    if isinstance(data, dict):
        data = data.get('content') or data.get('items') or []
    names = {p.get('name') for p in data if isinstance(p, dict) and p.get('name')} if isinstance(data, list) else set()
    if get_config()['profile'] not in names:
        logger.warning(f"⚠️ В списке профилей CTL нет своего профиля ({len(names)} имён) — отсев по профилю пропущен")
        return None
    return names


def dangling_events(events, sources, entities, profiles):
    """Ключи событий с висячими ссылками: {ключ: (причина, [workflow])}.

    Ключ — `профиль/сущность/статистика` из расписания событий workflow. Сущности нет в
    `/v4/api/entity` или профиля нет в `/v4/api/profile` — CTL на запрос статистики
    ответит 422, и сенсор событий будет спрашивать это на каждой проверке.
    """
    out = {}
    for key in events:
        prf, eid = key.split('/')[:2]
        if profiles is not None and prf not in profiles:
            out[key] = (f'профиля {prf} нет в CTL', sources.get(key, []))
        elif int(eid) not in entities:
            out[key] = (f'сущности {eid} нет в CTL', sources.get(key, []))
    return out


with DAG(f'CTL.{get_config()["profile"]}.loader',
    tags=['CTL', 'CTL_agent', 'logger'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval=str2timedelta(get_config().get('loader_interval','minutes=5')),
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
    on_failure_callback=partial(on_callback, level='DAG'),
    on_success_callback=partial(on_callback, level='DAG'),
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    doc_md=__doc__,
) as dag:
    
    
    @task(pool='ctl_pool')
    def chk_ctl():
        return chk_any_conn('ctl')
   
    
    # @task_group
    # def ctl_load():
    
    @task(pool='ctl_pool')
    def load_profile(**context):
        """### Загрузка профиля CTL

        Получает информацию о текущем профиле из API CTL по имени.
        Сохраняет данные в переменную `ctl_profile` через `ctl_obj_save(var=True)`,
        чтобы другие DAG'и могли получить к ним доступ.

        **Источник:** `/v4/api/profile/name/{profile_name}`  
        **Сохранение:** `ctl_profile` 
        """            
        data = ctl_api(f'/v4/api/profile/name/{get_config()["profile"]}')
        
        # Сохраняем в S3
        load_obj_save('ctl_profile', data, var=True, skip=True)


    def entity_subtree(flat, root):
        """Сущности поддерева root: обход вниз по parentId плоского списка CTL.

        Порядок фиксирован — в ширину, дети по возрастанию id. Это важнее красоты: на
        порядке стоит сравнение MD5 в ctl_obj_save, и стоит ему зависеть от порядка ответа
        CTL, как объект переписывался бы каждый круг. От прежнего обхода дерева порядок
        отличается, поэтому на первом прогоне после выкладки объект перезапишется один раз.
        """
        kids = {}
        for eid, ent in flat.items():
            parent = ent.get('parentId')
            if parent is not None:
                kids.setdefault(int(parent), []).append(eid)

        out, queue = {}, [root]
        while queue:
            eid = queue.pop(0)
            if eid in out or eid not in flat:
                continue
            out[eid] = flat[eid]
            queue.extend(sorted(kids.get(eid, [])))
        return out

    @task(pool='ctl_pool')
    def load_entities(**context):
        # Полный список сущностей, а не поиск по дереву.
        #
        # Раньше здесь стоял `/v4/api/entity/tree?search=<корень>&offset=0&limit=<ctl_limit>`
        # и обход `kidz`. Ручка поисковая, и свежесозданные сущности в неё не попадают:
        # 17.09.2026 на альфе в поддереве 941010000 было 682 сущности, а запрос устойчиво
        # отдавал 680 — не хватало ровно двух самых новых детей NONHR_STG (941010665 и
        # 941010666, последние id из 152). Уровни 0 и 1 приходили целыми, обрезка по
        # limit исключена (682 < 1000) — это отставание поискового индекса CTL, и
        # экспорт `CTL_<профиль>.yml` молча жил без этих сущностей больше суток.
        # `/v4/api/entity` читает таблицу: те же 682 узла, обе новые на месте.
        #
        # Лишнего запроса нет: полный список и раньше выгружался каждый круг для
        # `ctl_entities_all` (там его теперь читает load_events).
        root = int(get_config()['root_entity'])
        data = ctl_api('/v4/api/entity')
        load_obj_save('ctl_entities_all', data, var=False)

        flat = {int(d['id']): d for d in data}
        eids = entity_subtree(flat, root)

        # Сверка вместо молчания: сущности «нашего» семейства id вне поддерева. Штатно это
        # то, что выше корня (на альфе — 941000000 HR с прямыми детьми, 20 штук). Скачок
        # числа значит, что кому-то завели сущность мимо нашего корня.
        family = str(root)[:4]
        outside = sorted(i for i in flat if str(i).startswith(family) and i not in eids)
        add_note(f"🌳 поддерево {root}: {len(eids)}; семейство {family}…: "
                 f"{len(eids) + len(outside)}, вне поддерева {len(outside)}",
                 context, level='DAG,Task')
        logger.info("вне поддерева %s…: %s", family, outside[:50])

        # Сохраняем в S3
        load_obj_save('ctl_entities', eids, var=True, skip=True)
        
        
    @task(pool='ctl_pool')
    def load_categories(**context):
        """### Загрузка дерева категорий

        Выгружает все категории из CTL и строит иерархическое дерево,
        начиная с корневой (`root_category`). Фильтрует только те категории,
        которые принадлежат иерархии корня (включая потомков).

        Особое внимание — категории `ue_category`, которая сохраняется отдельно.

        **Функционал:**
        - Трёхпроходная сборка дерева (на случай неупорядоченных данных).
        - Добавление `parent_name` для удобства.
        - Сохранение в `ctl_categories` (публично).

        **Источник:** `/v4/api/category`  
        **Сохранение:** `ctl_categories`, `ctl_ue_category`
        """            
        # all_cats = ctl_api('/v5/api/category/m')
        all_cats = ctl_api('/v4/api/category')
        categories = {}

        # Три прохода для построения дерева (на случай, что родители идут после детей)
        for _ in range(3):
            for item in all_cats:
                cat_id = item['id']
                parent_id = item.get('parentId')

                if item['name'] == get_config()['root_category'] or (parent_id and parent_id in categories):
                    if cat_id not in categories:
                        categories[cat_id] = {**item}

        # Добавляем parent_name
        for cat in categories.values():
            parent_id = cat.get('parentId')
            if parent_id and parent_id in categories:
                cat['parent_name'] = categories[parent_id]['name']
                
            # if cat['name'] == config['root_category']:
            #     add_note(f"🔍 ctl_root_category: {cat}", context)
            #     load_obj_save("ctl_root_category", cat, var=True)
            
            if cat['name'] == get_config()['ue_category']:
                add_note(f"🔍 ctl_ue_category: {cat}", context)
                # Сохраняем в S3
                load_obj_save("ctl_ue_category", cat, var=True)
                        

        logger.info(f"🔍 ctl_categories: {categories}")
        context['ti'].xcom_push(key=f'categories', value=categories)
        
        # Сохраняем в S3
        load_obj_save('ctl_categories', categories, var=True, skip=True)


    @task(pool='ctl_pool')
    def load_workflows(**context):
        """### Загрузка и нормализация workflow'ов

        Для каждой категории из `ctl_categories` выгружает все workflows.
        Нормализует структуру:
        - Параметры → словарь `param: value`.
        - Уведомления → `status: emails`.
        - События → список `(entity_id, profile, stat_id, active)`.

        Также:
        - Собирает сущности, участвующие в событиях.
        - Строит иерархию имён сущностей (`Parent/child`).
        - Сохраняет `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events`.

        **Источник:** `/v4/api/wf?category_id={id}`  
        **Сохранение:** `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events` (все публично)
        """

        category_ids = ctl_obj_load('ctl_categories') or {}
        if not category_ids:
            msg = '❌ ctl_categories не загружены'
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)

        ids = ','.join(category_ids.keys())
        data = ctl_api('/v5/api/wf/extended', 'get', {'category_ids': f'[{ids}]' })
        md5  = md5_hash(data)
        wfs = {j['wf']['id']: ctl_wf_norm(j['wf'], j.get('connectedEntities', [])) for j in data}

        # Усохший список не сохраняем. Фабрика строит даги из этой же Variable, а Airflow
        # считает даг пропавшим, если разбор файла его не вернул, и удаляет строку из
        # serialized_dag (deactivate_stale_dags). То есть короткий ответ CTL молча убил бы
        # часть дагов; лучше упасть и оставить прежние данные в силе.
        pct = int(get_config().get('wf_shrink_pct', 10))
        was = ctl_obj_load('ctl_workflows') or {}
        if not wfs:
            msg = '❌ CTL вернул пустой список workflow — ctl_workflows не трогаем'
            add_note(msg, context, level='DAG,Task')
            raise Exception(msg)
        if was and len(wfs) < len(was) * (100 - pct) / 100:
            msg = (f'❌ Список workflow усох: было {len(was)}, пришло {len(wfs)} '
                   f'(порог {pct}%) — ctl_workflows не перезаписываем')
            add_note(msg, context, level='DAG,Task')
            raise Exception(msg)

        # Счётчик пишем ДО сохранения: load_obj_save при неизменившихся данных бросает
        # AirflowSkipException, и всё, что стоит после него, не выполнится никогда.
        # Счётчик для фабрики: она читает его отдельной Variable и сверяет с тем, что
        # прочитала сама. Если ctl_workflows подменился копией из S3, расхождение видно
        prf = get_config()['profile']
        cover = wf_coverage(wfs, category_ids, prf)
        # Тот же отбор, что у фабрики (ctl_worker._wf_eligible): свой профиль и dummy
        eligible = sum(1 for w in wfs.values()
                       if w.get('profile') == prf and w.get('engine') == AF_ENGINE
                       and not w.get('deleted', False))
        ctl_obj_save('ctl_workflows_stat', {
            'count': len(wfs),
            'eligible': eligible,
            'orphan': len(cover['orphan']),
            'double': len(cover['double']),
            'archive_live': len(cover['archive_live']),
            'profile': prf,
            'md5': md5,
            'saved': str(pendulum.now(get_config()['tz']))[:19],
        }, var=True)

        # Потоки вне присмотра — в заметку рана, полный список в лог. Таск не роняем: один
        # криво заведённый поток остановил бы обновление снимков, от которых живут сенсор
        # и фабрика дагов
        for kind, title in (('orphan', '⚠️ Потоки вне присмотра'),
                            ('double', '⚠️ Потоки и в Airflow, и в UE'),
                            ('archive_live', '⚠️ Архивные потоки на расписании')):
            if cover[kind]:
                logger.warning("%s: %d: %s", title, len(cover[kind]), cover[kind])
                top = dict(list(cover[kind].items())[:10])
                if len(cover[kind]) > len(top):
                    top['…'] = f"и ещё {len(cover[kind]) - len(top)}; полный список в логе таска"
                add_note(top, context, level='DAG,Task', title=f'{title}: {len(cover[kind])}')

        # Сохраняем в S3
        load_obj_save('ctl_workflows', wfs, var=True, skip=True)



    @task(pool='ctl_pool')
    def load_workflows_old(**context):
        """### Загрузка и нормализация workflow'ов

        Для каждой категории из `ctl_categories` выгружает все workflows.
        Нормализует структуру:
        - Параметры → словарь `param: value`.
        - Уведомления → `status: emails`.
        - События → список `(entity_id, profile, stat_id, active)`.

        Также:
        - Собирает сущности, участвующие в событиях.
        - Строит иерархию имён сущностей (`Parent/child`).
        - Сохраняет `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events`.

        **Источник:** `/v4/api/wf?category_id={id}`  
        **Сохранение:** `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events` (все публично)
        """
        # category_ids = context['ti'].xcom_pull(key=f'categories', task_ids=f'ctl_load.load_categories')
        category_ids = ctl_obj_load('ctl_categories') or {}
        wfs_old = ctl_obj_load('ctl_workflows') or {}

        
        # Workflows
        wfs ={}
        change = False
        for c in category_ids.keys():
            data = ctl_api(f'/v4/api/wf?category_id={c}', skip=False)
            for j in data:
                if j.get('deleted'): continue
                
                wid = j['id']
                
                wfExt = ctl_api(f'/v4/api/wf/{wid}/export', skip=False) or []
                # con = ctl_api(f'/v4/api/wf/{wid}/entity') or []
                hash = wfs_old.get(str(wid), {}).get('hash')
                if wfExt['hash'] == hash:
                    # logger.debug(f"📋 ctl_workflows no changes: {wid} {hash}")
                    wfs[wid] = wfs_old[str(wid)]
                else:
                    logger.debug(f"🔍 ctl_workflows : {wid} {hash} != {wfExt['hash']}")
                    wfExt['wfExt']['wf']['date'] = wfExt['date']
                    wfExt['wfExt']['wf']['hash'] = wfExt['hash']
                    wfs[wid] = ctl_wf_norm(wfExt['wfExt']['wf'], wfExt['wfExt']['connectedEntities'])
                    change = True

        # Сохраняем в S3
        if change:
            ctl_obj_save('ctl_workflows', wfs, var=True)
            add_note(f"🔍 ctl_workflows: {len(wfs)} / {readable_size(len(str(wfs)))}", context, level='DAG,Task')
        else:
            msg =f"⚠️ ctl_workflows no changes: {len(wfs)}"
            add_note(msg, context, level='DAG,Task')
            raise AirflowSkipException(msg)
        
        
    @task(pool='ctl_pool')
    def load_events(**context):
        
        wfs = ctl_obj_load('ctl_workflows') or {}
        if not wfs:
            msg = '❌ ctl_workflows не загружены'
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)


        # Events
        entity_events = set()
        events = {}
        sources = {}  # ключ события → какие workflow его ждут: для заметки о висячих
        for wf in wfs.values():
            for e in wf.get('wf_event_sched',[]):
                entity_events.add(int(e.split('/')[1]))
                events[e] = wf.get('scheduled', False) and events.get(e, True)
                sources.setdefault(e, []).append(f"{wf.get('id')} {wf.get('name', '')}")

        eids = { int(k):v for k,v in (ctl_obj_load('ctl_entities') or {}).items() }
                
        # Полный список сущностей выгружает load_entities этим же кругом — берём готовый
        # из S3, а не тянем второй раз: на альфе это 399 804 записи, 80 МБ за запрос.
        # Список идёт параллельной задачей, поэтому в худшем случае он от прошлого круга —
        # для поиска висячих ссылок это допустимо. Пусто (первый запуск, задача упала) —
        # спрашиваем CTL сами, иначе все события окажутся «висячими».
        data = ctl_obj_load('ctl_entities_all') or ctl_api('/v4/api/entity')

        data = { int(d['id']):d for d in data }

        # Висячие ссылки: сущность или профиль события в CTL не существуют. Сенсор событий
        # спрашивал бы их на каждой проверке и получал 422 — на alpha 14.09.2026 это 13 ключей
        # из 347, и каждый ответ ctl_api дописывал заметкой к задаче и рану
        dangling = dangling_events(events, sources, data, ctl_profile_names())
        if dangling and len(dangling) > DANGLING_MAX_SHARE * len(events):
            add_note(f"⚠️ Висячих ссылок на события {len(dangling)} из {len(events)} — больше "
                     f"{DANGLING_MAX_SHARE:.0%}, ответу CTL не верим, список событий не трогаем",
                     context, level='DAG,Task')
        elif dangling:
            rows = [f"| `{k}` | {r} | {'; '.join(w)} |" for k, (r, w) in sorted(dangling.items())]
            add_note("| Событие | Почему | Кто ждёт (workflow) |\n|---|---|---|\n" + "\n".join(rows[:40]),
                     context, level='DAG,Task',
                     title=f"⚠️ Висячие ссылки на события: {len(dangling)} — не опрашиваются, чинить в CTL")
            logger.warning(f"⚠️ Висячие ссылки на события: {sorted(dangling)}")
            events = {k: v for k, v in events.items() if k not in dangling}

        # Сохраняем в S3. «Без изменений» — только если не изменилось ничего из трёх: раньше
        # неизменные события уводили задачу в skip раньше, чем обновлялись имена сущностей
        changed = load_obj_save('ctl_events', events, var=True)
        
        eids_parents = [int(e['parentId']) for e in eids.values()]
        
        events_parents = [int(data[e]['parentId']) for e in entity_events if data.get(e)]
        
        all_keys = set(eids.keys()) | entity_events | set(eids_parents) | set(events_parents)
        
        # Извлекаем имя, берем часть после # и оставляем только ASCII
        enames = {
            e: ''.join(c for c in data.get(e,{}).get('name','_Not_found_').split('#')[-1] if ord(c) < 128).strip() or str(e)  
            for e in all_keys if e > 0
        }
        # Сохраняем в S3
        changed = load_obj_save('ctl_enames', enames, var=True) or changed

        
        entity_events = { e: enames.get(int(e), '_Not_found_') for e in sorted(entity_events) }
        # Сохраняем в S3
        changed = load_obj_save('ctl_entity_events', entity_events, var=True) or changed
        if not changed:
            raise AirflowSkipException('⚠️ события и имена сущностей не изменились')

        # return wfs
    
    @task(pool='ctl_pool')
    def load_ue_events(**context):
        """### Загрузка событий UE-категории

        Выгружает загрузки (loadings) из категории `ue_category` за последние N дней.
        Используется для мониторинга активности внешних систем.

        **Фильтрация:**
        - По `category_ids` (ID `ue_category`).
        - При наличии `ctl_days` — по дате старта.

        **Сохранение:** не сохраняется напрямую, только логируется.

        **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        """
        ue_cat = ctl_obj_load('ctl_ue_category')
        if not ue_cat:
            msg = "❌ No ue_category found"
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)

        
        prm = {'category_ids': f'[{ue_cat["id"]}]'}

        if get_config().get('ctl_days') > 0:
            prm['start'] =  pendulum.now(get_config()['tz']).subtract(days=get_config()['ctl_days']).start_of('day').int_timestamp * 1000

        data = ctl_loading_load(prm, save=False)
        # Сохраняем в S3
        load_obj_save('ctl_ue_events', data, var=False, skip=True)

    @task(pool='ctl_pool')
    def load_prf_events(**context):
        """### Загрузка событий по профилю

        Выгружает загрузки (loadings), связанные с текущим профилем, за последние N дней.
        Используется для аудита и анализа активности CTL.

        **Фильтрация:**
        - По `profile_ids`.
        - При наличии `ctl_days` — по дате старта.

        **Сохранение:** не сохраняется напрямую, только логируется.

        **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        """
        profile = ctl_obj_load('ctl_profile')
        if not profile:
            msg = "❌ No profile found"
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)
        
        prm = {'profile_ids': f'[{profile["id"]}]'}

        if get_config().get('ctl_days') > 0:
            prm['start'] = pendulum.now(get_config()['tz']).subtract(days=get_config()['ctl_days']).start_of('day').int_timestamp * 1000

        data = ctl_loading_load(prm, save=False)
        # Сохраняем в S3
        load_obj_save('ctl_prf_events', data, var=False, skip=True)

    @task(pool='ctl_pool')
    def load_enames(**context):
        pass
        
    chk_ctl() >> [
        load_profile(),
        load_categories(),
        load_entities(),
        load_workflows(),
        load_events(),
        load_ue_events(),
        load_prf_events(),
    ]
    # [profile, entities , categories, ue_events, ctl_events]
    # load_entities >> load_workflows
    # load_categories >> load_workflows 
    # load_categories >> load_ue_events
    # load_profile >> load_ctl_events
    # load_workflows >> load_enames

    # chk_ctl() >> ctl_load() #>> EmptyOperator(task_id='end') 