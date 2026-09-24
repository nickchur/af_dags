"""### 🧭 DAG: Навыки агента для MCP-эндпоинта
*2026-09-24 11:23 MSK · v1.3 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

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

**Документация дагов** — второй таск, `publish_docs`, тем же путём: все остальные `.md`
(README каталогов, QUICKSTART, ТЗ) для пункта UI Docs → DAG Docs (etl-core,
`plugins/dag_docs_plugin.py`).

| Что | Где |
|---|---|
| Текст документа `<путь>` | Variable `af_doc__<путь, где / заменён на __>` — только при `store_docs` |
| Оглавление: путь → sha256, размер, заголовок, `stored` | Variable `af_docs` (JSON) — всегда |

**Хранить ли тексты — решает контур** (форма запуска, `store_docs`, по умолчанию нет).
Вебсервер альфы читает тексты прямо из бакета дагов по `path` из оглавления, ему нужна
только оглавление; ~400 КБ документации в метабазе там ни к чему. Вебсерверу сигмы S3
запрещён — там тексты едут через Variables, и флаг включают запуском с `store_docs` и
`save_params`. `stored` в записи оглавления — лежит ли в Variables текст **этой** версии
файла: страница показывает из Variables только такие. `purge_docs` — разово удалить все
`af_doc__*` (например, на альфе после перехода на S3).

Расписание — параметр `schedule` (по умолчанию раз в 30 минут), сохраняется так же;
пусто — только ручной запуск.

Не публикуются: навыки (`*/skill/*.md` — они уже есть), `CLAUDE.md` и `CONTEXT.md`
(правила и карта для агента), `openspec/`, `testbed/`, скрытые каталоги.
"""

from datetime import datetime, timedelta, timezone
from logging import getLogger

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, ensure_pool, on_callback, saved_params, saved_schedule, store_params_task,
    )

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

#: Документация дагов для страницы Docs → DAG Docs. Тоже контракт с etl-core
#: (``plugins/dag_docs_plugin.py``).
DOC_PREFIX = 'af_doc__'
DOC_INDEX = 'af_docs'
#: Файлы для агента, а не для людей: правила репозитория и собранная карта.
DOC_SKIP_FILES = {'CLAUDE.md', 'CONTEXT.md'}
DOC_SKIP_DIRS = {'testbed', 'openspec', 'skill', '__pycache__'}

ensure_pool(TOOLS_POOL)

# Значения формы по умолчанию: код — запасной вариант, переменная — рабочий. Пишет
# переменную только запуск с save_params=True (таск params)
PARAMS_VAR = 'tools_mcp_skills_params'
SAVED = saved_params(PARAMS_VAR)
#: Разовые галочки: в переменную не сохраняются
ONE_SHOT = ('purge_docs',)
# Раз в 30 минут: настолько навык на эндпоинте может отстать от выложенных дагов.
# Работа дешёвая — без изменений в файлах записи нет вовсе
DEFAULT_SCHEDULE = '*/30 * * * *'


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


def find_docs(root):
    """``{путь: путь-файл}`` по ``**/*.md`` под ``root``, кроме навыков и файлов для агента.

    Ключ — путь от корня дагов через ``/``: он же имя документа на странице и в ссылках.
    """
    from pathlib import Path

    found = {}
    for path in sorted(Path(root).glob('**/*.md')):
        rel = path.relative_to(root)
        if path.name in DOC_SKIP_FILES or any(
            p.startswith('.') or p in DOC_SKIP_DIRS for p in rel.parts[:-1]
        ):
            continue
        found[rel.as_posix()] = path
    return found


def doc_key(rel: str) -> str:
    """Ключ Variable документа: ``er_export/README.md`` → ``af_doc__er_export__README.md``."""
    return DOC_PREFIX + rel.replace('/', '__')


def doc_title(text: str, fallback: str) -> str:
    """Первый заголовок ``# …``, иначе имя файла."""
    for line in text.splitlines():
        if line.startswith('# '):
            return line[2:].strip()
    return fallback


def plan(found, index, root, max_bytes=SKILL_MAX_BYTES, titles=False):
    """Что сделать: ``(новое оглавление, {имя: текст на запись}, [имена на снятие], [пропущено])``.

    Отдельно от записи — чтобы решение проверялось тестом без Airflow. ``titles`` — класть
    в оглавление заголовок документа (для страницы DAG Docs).
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
        if titles:
            new_index[name]['title'] = doc_title(raw.decode('utf-8'), path.name)
        if (index.get(name) or {}).get('sha256') != sha:
            to_write[name] = raw.decode('utf-8')
    to_delete = sorted(set(index) - set(new_index))
    return new_index, to_write, to_delete, skipped


def docs_plan(found, index, root, store, purge=False):
    """План для документов: ``(оглавление, {путь: текст на запись}, [пути на снятие], [пропущено])``.

    Записи оглавления получают ``stored`` — лежит ли в Variables текст этой версии. Записи
    старого формата (до ``stored``) считаются сохранёнными: их тексты писал прежний даг.
    При ``purge`` сохранённого нет вовсе: всё удаляется до записи.
    """
    known = {} if purge else {k: v for k, v in index.items() if (v or {}).get('stored', True)}
    new_index, to_write, to_delete, skipped = plan(found, known, root, titles=True)
    if not store:
        # Не храним: писать нечего, а прежние тексты этой версии остаются валидными
        for rel, meta in new_index.items():
            meta['stored'] = (known.get(rel) or {}).get('sha256') == meta['sha256']
        return new_index, {}, [], skipped
    for meta in new_index.values():
        meta['stored'] = True
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
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    tags=['DataLab', 'tools', 'mcp'],
    catchup=False,
    is_paused_upon_creation=False,
    params={
        'store_docs': Param(
            bool(SAVED.get('store_docs', False)), type='boolean', title='Хранить тексты документов',
            description='Класть тексты .md в Variables af_doc__*. Нужно, где вебсервер не читает '
                        'S3 (сигма). Оглавление af_docs пишется всегда.',
        ),
        'purge_docs': Param(
            False, type='boolean', title='Удалить сохранённые тексты',
            description='Разово удалить все af_doc__* (в переменную не сохраняется).',
        ),
        'schedule': Param(
            SAVED.get('schedule', DEFAULT_SCHEDULE), type=['string', 'null'], title='Расписание',
            description='cron или пресет (@daily); пусто — только вручную. Применяется со следующего разбора',
        ),
        'save_params': Param(
            False, type='boolean', title='Сохранить параметры',
            description=f'Записать store_docs и schedule в {PARAMS_VAR}: по ним пойдут и плановые запуски.',
        ),
    },
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    on_failure_callback=on_callback,
)
def tools_mcp_skills():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет store_docs и schedule в переменную как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context, one_shot=ONE_SHOT)

    # NONE_FAILED: params штатно пропускает себя без save_params, а пропуск апстрима по
    # ALL_SUCCESS утянул бы в skip и публикацию
    @task(trigger_rule=TriggerRule.NONE_FAILED)
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

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def publish_docs(**context):
        from airflow.configuration import conf
        from airflow.models import Variable

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        root = conf.get('core', 'dags_folder')
        p = context['params']
        store, purge = bool(p.get('store_docs')), bool(p.get('purge_docs'))
        index = Variable.get(DOC_INDEX, default_var={}, deserialize_json=True) or {}
        new_index, to_write, to_delete, skipped = docs_plan(find_docs(root), index, root, store, purge)

        purged = _purge_docs() if purge else 0
        for rel, text in to_write.items():
            Variable.set(doc_key(rel), text,
                         description=f"Документация дагов для Docs → DAG Docs: {rel}")
        for rel in to_delete:
            Variable.delete(doc_key(rel))
        # Оглавление — последним: страница верит ему и ссылаться оно должно на записанное
        if new_index != index:
            Variable.set(DOC_INDEX, new_index, serialize_json=True,
                         description="Оглавление документации дагов (Docs → DAG Docs): путь → sha256, размер, заголовок")

        stored = sum(1 for m in new_index.values() if m['stored'])
        title = (f"DAG Docs: {len(new_index)}, тексты {'хранятся' if store else 'не хранятся'} "
                 f"({stored}), записано {len(to_write)}, снято {len(to_delete)}")
        rows = [f"🧹 удалено сохранённых текстов: {purged}"] if purge else []
        rows += [f"✏️ `{r}`" for r in sorted(to_write)] + [f"🗑️ `{r}`" for r in to_delete]
        rows += [f"❌ {s}" for s in skipped]
        add_note("\n".join(rows) or "без изменений", context, level='DAG,task', title=title)
        if skipped:
            raise ValueError("Документы пропущены: " + "; ".join(skipped))
        return {'docs': len(new_index), 'stored': stored, 'written': sorted(to_write),
                'deleted': to_delete, 'purged': purged}

    def _purge_docs():
        """Удаляет все Variables ``af_doc__*``. Возвращает, сколько удалено."""
        from airflow.models import Variable
        from airflow.utils.session import create_session

        with create_session() as session:
            rows = session.query(Variable).filter(Variable.key.like(f'{DOC_PREFIX}%')).all()
            for row in rows:
                session.delete(row)
            return len(rows)

    params_done = save_params()
    params_done >> [publish(), publish_docs()]


tools_mcp_skills()
