"""
### 🫀 DAG: задачи выполняются
*2026-10-01 22:26 MSK · v2.3 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в час два таска подряд:
- `dummy_task` (`EmptyOperator`) — шедулер отмечает его успешным сам, до воркера он не
  доходит: зелёный — шедулер жив и разбирает раны;
- `ping` — настоящая задача на воркере. В заметке — под, сколько шедулер думал между
  `dummy_task` и постановкой `ping` в очередь и сколько `ping` ждал воркера в очереди.

Красный ран по `dagrun_timeout` — за 50 минут не выполнилось: не зелёный `dummy_task` —
стоит шедулер, не зелёный `ping` — задачи не доходят до воркера. Разбор —
`tools_system_health` и `tools_log_events` за это время. До 28.09.2026 — ручной даг для
проверки Markdown в UI.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
try:
    from plugins import af_meta  # type: ignore
except ImportError:
    from CI06932748.tools import af_meta  # type: ignore

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)


def _sec(a, b):
    """Секунды от a до b; '?' если одной из отметок нет."""
    return f"{(b - a).total_seconds():.0f} с" if a and b else '?'


@dag(
    dag_id='tools_dummy',
    doc_md=__doc__,
    description='Пульс: жив ли шедулер и доходит ли задача до воркера, сколько ждёт в очереди',
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. test_connections.py)
        'priority_weight': 900,
        'weight_rule': 'absolute',
        'execution_timeout': timedelta(minutes=5),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 22, tzinfo=timezone.utc),
    schedule='3 * * * *',
    # Меньше часа: не выполнившийся ран краснеет до следующего, а не копится за ним
    dagrun_timeout=timedelta(minutes=50),
    tags=['DataTools', 'tools', 'AutoQA'],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    on_failure_callback=on_callback,
)
def tools_dummy():

    @task(task_id='ping')
    def ping(**context):
        """Отмечает, где выполнилась задача и сколько ждала шедулер и воркер."""
        # Airflow 3: у RuntimeTaskInstance нет queued_dttm и hostname, у DagRun — get_task_instance;
        # моменты обеих задач — из метабазы одним запросом
        ti, dr = context['ti'], context['dag_run']
        rows = {r['task_id']: r for r in af_meta.sql(
            "SELECT task_id, hostname, queued_dttm, start_date, end_date FROM task_instance"
            " WHERE dag_id = %s AND run_id = %s AND task_id IN ('ping', 'dummy_task') AND map_index = -1",
            (dr.dag_id, dr.run_id))}
        me, empty = rows.get('ping') or {}, rows.get('dummy_task') or {}
        add_note(
            f"✅ выполнено на {me.get('hostname') or '?'}: шедулер {_sec(empty.get('end_date'), me.get('queued_dttm'))}, "
            f"очередь {_sec(me.get('queued_dttm'), me.get('start_date') or ti.start_date)}",
            context, level='task',
        )

    EmptyOperator(task_id='dummy_task') >> ping()


tools_dummy()
