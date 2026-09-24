"""
### 🧪 DAG: Проверка отображения Markdown
*2026-09-24 11:17 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Пустой даг: одна задача `EmptyOperator`. Нужен, чтобы посмотреть, как Airflow UI рисует
`doc_md` — таблицы, цитаты, списки, ссылки, блоки кода — и проверить колбэки
(`on_callback`) на ране, который ничего не делает. Запускается только вручную.

С 24.09.2026 лежит среди служебных дагов `tools/` (раньше — в S3-инструментах альфы) и
оформлен по их стандарту: владелец DataLab, пул `TOOLS_POOL`, приоритет 900.

---

| **Параметр** | *Значение* | Описание |
| :--- | :---: | :--- |
| `timeout` | 30 | Время ожидания |
| `retries` | 0 | Количество попыток |
| `retry_delay` | 30 | Время между попытками |

---

> Данный DAG предназначен для тестирования и отладки.

- один
    - *один_один*
- два
- три

🔗 [Открыть лог в Elastic](https://logs.company.com)

```json
{
    "key": "value"
}
```
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

try:
    from plugins.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore

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
        'on_success_callback': on_callback,
    },
    start_date=datetime(2026, 1, 22, tzinfo=timezone.utc),
    schedule=None,
    tags=['DataLab', 'tools', 'dummy'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
)
def tools_dummy():
    EmptyOperator(task_id='dummy_task', doc_md=__doc__)


tools_dummy()
