"""
### 🫀 DAG: задачи выполняются
*2026-09-28 10:38 MSK · v2.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в час ставит одну пустую задачу `ping` и ждёт, что воркер её выполнит. Зелёный ран —
задачи доходят до воркера и отрабатывают; в заметке — под и сколько задача ждала в очереди.
Красный ран по `dagrun_timeout` — за 50 минут задача так и не выполнилась: шедулер не
поставил её в очередь или воркеры её не взяли. Разбор — `tools_system_health` и
`tools_log_events` за это время.

`EmptyOperator` для этого не годится: шедулер отмечает его успешным сам, не отправляя на
воркер. До 28.09.2026 — ручной даг для проверки Markdown в UI.
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)


@dag(
    dag_id='tools_dummy',
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. show_connections.py)
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
        """Отмечает, где и через сколько после постановки в очередь выполнилась задача."""
        ti = context['ti']
        wait = f", в очереди {(ti.start_date - ti.queued_dttm).total_seconds():.0f} с" if ti.queued_dttm else ''
        add_note(f"✅ выполнено на {ti.hostname}{wait}", context, level='task')

    ping()


tools_dummy()
