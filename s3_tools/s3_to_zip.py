"""### 🗜️ DAG: ZIP-архив из S3 в S3
*2026-10-19 17:14 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в сутки (по умолчанию 01:30 MSK) архивирует вчерашнее по MSK: на каждый путь из `src` — свой ZIP64 в `dst`.
Сборка потоковая — без диска воркера и без буфера под весь архив; исходники не трогает. По умолчанию — логи задач
в `archive/` бакета логов (срок хранения — общий для бакета). Пустой день у src — экземпляр skipped.
Значения формы — из переменной `tools_s3_to_zip_params`, сохранить — галочка `save_params` (даты разовые).

**Таски:** `params` → `jobs` → `archive` (по экземпляру на src, по одному за раз).

Подробно: [s3_tools/readme.md — s3_to_zip](../../_plugin_dag_docs/?doc=s3_tools/readme.md#s3_to_zippys3_to_zippy)
"""
from datetime import datetime, timedelta, timezone

from airflow.configuration import conf
from airflow.sdk import Param, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowFailException

try:
    from plugins.s3_utils import s3_path_parse, s3_to_zip  # type: ignore
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, log_bucket_path, on_callback, readable_size, saved_params,
        saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.s3_utils import s3_path_parse, s3_to_zip  # type: ignore
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, log_bucket_path, on_callback, readable_size, saved_params,
        saved_schedule, store_params_task,
    )

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Значения по умолчанию для формы: код — запасной вариант, переменная — рабочий (механика log_cleanup)
PARAMS_VAR = 'tools_s3_to_zip_params'
# 01:30 MSK: вчерашние сутки MSK закрыты; cron в зоне start_date
DEFAULT_SCHEDULE = '30 1 * * *'
MSK = timezone(timedelta(hours=3))
# Префикс логов задач из s3://бакет/префикс; бакет и подключение даёт log_bucket_path
_LOG_PREFIX = conf.get('logging', 'remote_base_log_folder', fallback='').split('://', 1)[-1].partition('/')[2].strip('/')
DEFAULT_SRC = log_bucket_path(f'{_LOG_PREFIX}/' if _LOG_PREFIX else '')

SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


def _jobs(p, now):
    """Раскладка запуска по архивам: на каждый src — свой; вчера по MSK, имена с номерами, одна метка на ран."""
    srcs = [s.strip() for s in p['src'] if s and s.strip()]
    if not srcs:
        raise AirflowFailException('src пуст: нужен хотя бы один путь conn_id://бакет/путь')
    if len(srcs) > 1 and p['dst'].strip().lower().endswith('.zip'):
        raise AirflowFailException(f"src {len(srcs)}, а dst — один ключ .zip: при нескольких src dst должен быть папкой")
    d_from, d_to = p.get('date_from') or None, p.get('date_to') or None
    if not d_from and not d_to:
        d_from = d_to = (now.astimezone(MSK) - timedelta(days=1)).date().isoformat()
    ts = now.astimezone(timezone.utc).strftime('%Y%m%d_%H%M%S') if p.get('add_ts', True) else None
    jobs, seen = [], {}
    for s in srcs:
        sp = s3_path_parse(s)
        base = sp['prefix'] if sp['is_mask'] else sp['key']
        stem = base.rstrip('/').rsplit('/', 1)[-1] or sp['bucket']
        seen[stem] = seen.get(stem, 0) + 1
        jobs.append({'src': s, 'dst': p['dst'].strip(), 'name': stem if seen[stem] == 1 else f'{stem}_{seen[stem]}',
                     'ts': ts, 'date_from': d_from, 'date_to': d_to,
                     'max_bytes': int(float(p['max_size_gb']) * 1024**3), 'replace': bool(p['replace'])})
    return jobs


params = {
    'src': _param(
        'src', [DEFAULT_SRC] if DEFAULT_SRC else [],
        type='array', items={'type': 'string'}, minItems=1,
        description='Что архивировать: список conn_id://бакет/путь, на каждый — свой архив; '
                    'без маски — всё под путём, маска * ? — по ключу',
    ),
    'dst': _param(
        'dst', log_bucket_path('archive/'), type='string',
        description='Куда: папка с / — имя <папка src>[_<метка>].zip; ключ .zip — только при одном src',
    ),
    # Разовые: в переменную не сохраняются, иначе ежедневный ран застынет на одной дате
    'date_from': Param(None, type=['null', 'string'], format='date',
                       description='LastModified с (MSK, включительно); обе даты пусто — вчера по MSK'),
    'date_to': Param(None, type=['null', 'string'], format='date',
                     description='LastModified по (MSK, включительно); обе даты пусто — вчера по MSK'),
    'add_ts': _param('add_ts', True, type='boolean',
                     description='Добавлять к имени метку _YYYYMMDD_HHMMSS (UTC, старт рана); без неё повтор упрётся в replace'),
    'max_size_gb': _param('max_size_gb', 20, type='number', minimum=0,
                          description='Предохранитель на каждый архив: сырой объём больше — экземпляр падает до записи; 0 — без лимита'),
    'replace': _param('replace', False, type='boolean', description='Перезаписать архив, если ключ уже есть'),
    'schedule': _param('schedule', DEFAULT_SCHEDULE, type='string',
                       description='Расписание: cron (MSK) или пресет @daily; пусто — только вручную. Применяется со следующего парсинга'),
    # Разовое действие, а не настройка: в переменную не сохраняется и берётся всегда из кода
    'save_params': Param(False, type='boolean',
                         description='True — сохранить параметры этого запуска как значения по умолчанию'),
}


@dag(
    doc_md=__doc__,
    description='Раз в сутки — вчерашние объекты S3 (по умолчанию логи) в ZIP-архивы в S3, по архиву на путь',
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab',
                 'Nick Churkin': 'mailto:NSChurkin@sber.ru'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        # Как у соседей по пулу: иначе вес по потомкам проигрывает большим дагам в scheduled
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # 20 ГБ при ~4 МБ/с на стенде — около полутора часов; потолок от зависшего S3
        'execution_timeout': timedelta(hours=6),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 10, 19, tzinfo=MSK),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    # Архивы по одному: нагрузка на воркер и S3 — как от одного
    max_active_tasks=1,
    on_failure_callback=on_callback,
    tags=['tools', 's3'],
    params=params,
)
def tools_s3_to_zip():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию (даты — разовые)."""
        return store_params_task(PARAMS_VAR, SAVED, context, one_shot=('date_from', 'date_to'))

    # NONE_FAILED: params штатно пропускает себя без save_params, ALL_SUCCESS утянул бы всё в skip
    @task(task_id='jobs', trigger_rule=TriggerRule.NONE_FAILED)
    def jobs(params=None, **context):
        """🧮 Раскладка по архивам: вчера по MSK, имена с номерами, одна метка на ран."""
        j = _jobs(params, datetime.now(timezone.utc))
        add_note({x['name']: x['src'] for x in j}, context)
        return j

    @task(map_index_template="{{ job['name'] }}")
    def archive(job, **context):
        """🗜️ Один src → один ZIP: обход, лимит, потоковая запись; итог — в заметке."""
        r = s3_to_zip(job['src'], job['dst'], job['date_from'], job['date_to'], job['max_bytes'], job['replace'],
                      name=job['name'], ts=job['ts'])
        add_note({**r, 'raw': readable_size(r['raw']), 'size': readable_size(r['size'])}, context)
        return r

    planned = jobs()
    save_params() >> planned
    archive.override(doc_md=archive.function.__doc__).expand(job=planned)


tools_s3_to_zip()
