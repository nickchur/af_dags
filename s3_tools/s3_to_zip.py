"""### 🗜️ DAG: ZIP-архив из S3 в S3
*2026-10-19 15:30 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Потоково собирает объекты S3 в один ZIP64 и кладёт в S3 — без диска воркера и без буфера под весь архив.
Исходники не трогает. По умолчанию — логи задач в `archive/` бакета логов (срок хранения — общий для бакета).

**Таски:** `archive`.

Подробно: [s3_tools/readme.md — s3_to_zip](../../_plugin_dag_docs/?doc=s3_tools/readme.md#s3_to_zippys3_to_zippy)
"""
from datetime import datetime, timedelta, timezone

from airflow.configuration import conf
from airflow.sdk import Param, dag, task

try:
    from plugins.s3_utils import s3_to_zip  # type: ignore
    from plugins.utils import add_note, log_bucket_path, readable_size  # type: ignore
except ImportError:
    from CI06932748.tools.s3_utils import s3_to_zip  # type: ignore
    from CI06932748.tools.utils import add_note, log_bucket_path, readable_size  # type: ignore

# Префикс логов задач из s3://бакет/префикс; бакет и подключение даёт log_bucket_path
_LOG_PREFIX = conf.get('logging', 'remote_base_log_folder', fallback='').split('://', 1)[-1].partition('/')[2].strip('/')


@dag(
    doc_md=__doc__,
    description='Потоково собрать объекты S3 (по умолчанию логи) в ZIP-архив в S3',
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab',
                 'Nick Churkin': 'mailto:NSChurkin@sber.ru'},
    default_args={'owner': 'DataLab (CI02420667)', 'retries': 1, 'retry_delay': timedelta(minutes=1)},
    start_date=datetime(2026, 10, 19, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=['tools', 's3'],
    params={
        'src': Param(log_bucket_path(f'{_LOG_PREFIX}/' if _LOG_PREFIX else ''), type='string',
                     description='Что архивировать: conn_id://бакет/путь; без маски — всё под путём, маска * ? — по ключу'),
        'dst': Param(log_bucket_path('archive/'), type='string',
                     description='Куда: conn_id://бакет/ключ.zip или папка с / — имя <папка src>_<время UTC>.zip'),
        'date_from': Param(None, type=['null', 'string'], format='date', description='LastModified с (UTC, включительно); пусто — без границы'),
        'date_to': Param(None, type=['null', 'string'], format='date', description='LastModified по (UTC, включительно); пусто — без границы'),
        'max_size_gb': Param(20, type='number', minimum=0, description='Предохранитель: сырой объём больше — ран падает до записи; 0 — без лимита'),
        'replace': Param(False, type='boolean', description='Перезаписать архив, если ключ уже есть'),
    },
)
def tools_s3_to_zip():

    @task
    def archive(params=None, **context):
        """Обходит ключи, проверяет лимит и потоково пишет архив; итог — в заметке."""
        r = s3_to_zip(params['src'], params['dst'], params['date_from'], params['date_to'],
                      int(params['max_size_gb'] * 1024**3), params['replace'])
        add_note({**r, 'raw': readable_size(r['raw']), 'size': readable_size(r['size'])}, context)
        return r

    archive()


tools_s3_to_zip()
