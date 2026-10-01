"""### 🪣 DAG: Просмотр бакетов S3
*2026-10-01 18:21 MSK · v1.3 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Выводит список бакетов для выбранного S3-подключения.

"""

from datetime import datetime, timedelta, timezone
from airflow.models import Param
from airflow.decorators import dag

from hrp_operators.s3_viewer_operator import HrpS3BucketViewerOperator  # type: ignore
from plugins.utils import get_conns_by_type  # type: ignore


@dag(
    doc_md=__doc__,
    description='Список бакетов выбранного S3-подключения',
    owner_links={
        'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab',
        'Korchagin Viacheslav': 'mailto:VYurKorchagin@sberbank.ru',
    },
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
        'retry_delay': timedelta(seconds=30),
    },
    start_date=datetime(2026, 1, 21, tzinfo=timezone.utc),
    schedule_interval=None,
    tags=['EDP_ETL', 'tools', 's3'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    params={
        'aws_conn_id': Param('s3', type='string', enum=get_conns_by_type(conn_type='aws'), title='ID подключения к S3', description='S3-подключение, чьи бакеты показать'),
    },
)
def tools_s3_bucket_viewer():
    HrpS3BucketViewerOperator(
        task_id='s3_bucket_list',
        aws_conn_id='{{ params.aws_conn_id }}',
    )


tools_s3_bucket_viewer()
