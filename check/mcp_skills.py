"""### 🧭 DAG: Навыки агента для MCP-эндпоинта
*2026-09-23 14:33 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Кладёт навыки агента из репозитория дагов (`*/skill/*.md`) в Airflow Variables, откуда
MCP-эндпоинт вебсервера отдаёт их ресурсами `airflow://skill/<имя>`.

Зачем посредник. Даги приезжают из S3 синхронизацией только в поды шедулера,
dag-processor'а и воркера; у вебсервера каталог дагов пуст ни на сигме, ни на альфе
(`entrypoint.sh`, ветка `webserver`), а читать S3 напрямую вебсерверу сигмы запрещено.
Воркер файлы уже видит, метабазу видят все — через неё навык и едет.

| Что | Где |
|---|---|
| Текст навыка `<имя>` | Variable `mcp_skill__<имя>` |
| Оглавление: имя → sha256, путь, размер | Variable `mcp_skills` (JSON) |

Имя навыка — имя файла без `.md`. Переменная переписывается, только если файл изменился;
навык, чей файл исчез, снимается вместе со своей переменной. Два файла с одним именем —
ошибка таска: какой из них отдавать, решать не нам.
"""

from datetime import datetime, timedelta, timezone
from logging import getLogger

from airflow.decorators import dag, task

try:
    from plugins.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore

logger = getLogger("airflow.task")

MSK = timezone(timedelta(hours=3))

#: Префикс переменной с текстом навыка и оглавление. Контракт с etl-core
#: (``mcp_skill.py``): меняются только вместе.
SKILL_PREFIX = 'mcp_skill__'
SKILL_INDEX = 'mcp_skills'
#: Имя попадает в URI ресурса и в ключ переменной: только то, что безопасно в обоих.
SKILL_NAME_RE = r'^[a-z0-9][a-z0-9-]{0,63}$'
#: Навык ctl-worker — 20 КБ. Потолок — от случайно подложенного бинарника, а не от
#: навыков: агенту больше и не прочитать за раз.
SKILL_MAX_BYTES = 256 * 1024

ensure_pool(TOOLS_POOL)


def find_skills(root):
    """``{имя: путь}`` по ``*/skill/*.md`` под ``root``. Дубли имён — ``ValueError``.

    Скрытые каталоги и ``testbed`` пропускаются: их не разбирает и Airflow
    (``.airflowignore``), а навык из стенда на контур попадать не должен.
    """
    import re
    from pathlib import Path

    found, dups = {}, []
    for path in sorted(Path(root).glob('**/skill/*.md')):
        rel = path.relative_to(root)
        if any(p.startswith('.') or p == 'testbed' for p in rel.parts[:-1]):
            continue
        name = path.stem.lower()
        if not re.match(SKILL_NAME_RE, name):
            logger.warning("Навык %s пропущен: имя не подходит под %s", rel, SKILL_NAME_RE)
            continue
        if name in found:
            dups.append(f"{name}: {found[name].relative_to(root)} и {rel}")
            continue
        found[name] = path
    if dups:
        raise ValueError("Навыки с одинаковым именем: " + "; ".join(dups))
    return found


def plan(found, index, root, max_bytes=SKILL_MAX_BYTES):
    """Что сделать: ``(новое оглавление, {имя: текст на запись}, [имена на снятие], [пропущено])``.

    Отдельно от записи — чтобы решение проверялось тестом без Airflow.
    """
    import hashlib

    new_index, to_write, skipped = {}, {}, []
    for name, path in found.items():
        raw = path.read_bytes()
        if len(raw) > max_bytes:
            skipped.append(f"{name}: {len(raw)} байт > {max_bytes}")
            continue
        sha = hashlib.sha256(raw).hexdigest()
        new_index[name] = {'sha256': sha, 'path': str(path.relative_to(root)), 'bytes': len(raw)}
        if (index.get(name) or {}).get('sha256') != sha:
            to_write[name] = raw.decode('utf-8')
    to_delete = sorted(set(index) - set(new_index))
    return new_index, to_write, to_delete, skipped


@dag(
    doc_md=__doc__,
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 2,
        # Как у соседей по пулу: выше регрессии, ниже агента CTL (см. show_connections.py)
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # Чтение пары файлов и запись переменных — секунды; потолок про зависание
        'execution_timeout': timedelta(minutes=5),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    # Раз в 30 минут: настолько навык на эндпоинте может отстать от выложенных дагов.
    # Работа дешёвая — без изменений в файлах записи нет вовсе.
    schedule='*/30 * * * *',
    tags=['DataLab', 'tools', 'mcp'],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    on_failure_callback=on_callback,
)
def tools_mcp_skills():

    @task
    def publish(**context):
        from airflow.configuration import conf
        from airflow.models import Variable

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        root = conf.get('core', 'dags_folder')
        index = Variable.get(SKILL_INDEX, default_var={}, deserialize_json=True) or {}
        new_index, to_write, to_delete, skipped = plan(find_skills(root), index, root)

        for name, text in to_write.items():
            Variable.set(
                SKILL_PREFIX + name, text,
                description=f"Навык агента {name} для MCP (airflow://skill/{name}), из {new_index[name]['path']}",
            )
        for name in to_delete:
            Variable.delete(SKILL_PREFIX + name)
        # Оглавление — последним: эндпоинт верит ему, и ссылаться оно должно только на
        # уже записанное
        if new_index != index:
            Variable.set(SKILL_INDEX, new_index, serialize_json=True,
                         description="Оглавление навыков агента для MCP: имя → sha256, путь, размер")

        rows = ["| Навык | Файл | Байт | |", "|---|---|---:|---|"]
        for name, meta in sorted(new_index.items()):
            mark = '✏️ записан' if name in to_write else '✅ без изменений'
            rows.append(f"| `{name}` | `{meta['path']}` | {meta['bytes']} | {mark} |")
        rows += [f"| `{name}` | — | — | 🗑️ снят |" for name in to_delete]
        rows += [f"| ❌ {s} | | | пропущен |" for s in skipped]
        title = f"Навыки MCP: {len(new_index)}, записано {len(to_write)}, снято {len(to_delete)}"
        add_note("\n".join(rows), context, level='DAG,task', title=title)
        if skipped:
            raise ValueError("Навыки пропущены: " + "; ".join(skipped))
        return {'skills': sorted(new_index), 'written': sorted(to_write), 'deleted': to_delete}

    publish()


tools_mcp_skills()
