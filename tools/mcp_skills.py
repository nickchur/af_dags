"""### 🧭 DAG: Навыки агента для MCP-эндпоинта
*2026-09-25 19:32 MSK · v1.4 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

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

**Документация дагов** — второй таск, `publish_docs`: все остальные `.md` (README каталогов,
QUICKSTART, ТЗ) для пункта UI Docs → DAG Docs и ресурсов MCP `airflow://doc/{path}` (etl-core,
`dag_docs.py`).

| Что | Где |
|---|---|
| Текст документа `<путь>` | `docs/<путь>` в корне бакета логов |
| Оглавление: путь → sha256, размер, заголовок, `at` (когда записан текст) | Variable `af_docs` (JSON) |

Тексты — в бакете логов с 25.09.2026: вебсервер читает их через лог-сервер воркера одним
путём на обоих контурах (до этого — Variables `af_doc__*` на сигме по флагу `store_docs` и
бакет дагов на альфе). У бакета один срок хранения на всё (`tools_log_cleanup`), поэтому текст
переписывается не только при смене файла, но и когда записи в оглавлении больше
`DOC_REFRESH_DAYS` (7 дней). `purge_docs` — разово удалить оставшиеся `af_doc__*`.

Навыки остаются в Variables: навык нужен и тогда, когда воркеров нет, а метабаза жива.

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

#: Документация дагов для страницы Docs → DAG Docs. Тоже контракт с etl-core (``dag_docs.py``):
#: оглавление в Variable, тексты в ``docs/`` бакета логов. ``af_doc__*`` — прежнее место
#: текстов, только для ``purge_docs``.
DOC_PREFIX = 'af_doc__'
DOC_INDEX = 'af_docs'
DOCS_ROOT = 'docs'
#: Срок хранения бакета логов один на всё (120 дней, на деве 30): текст старше этого
#: переписываем, чтобы живой документ не ушёл по сроку
DOC_REFRESH_DAYS = 7
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


def docs_plan(found, index, root, now):
    """План для документов: ``(оглавление, {путь: текст на запись}, [пути на снятие], [пропущено])``.

    На запись — изменившиеся и те, чей текст записан раньше ``now - DOC_REFRESH_DAYS``
    (поле ``at``): иначе неизменный документ ушёл бы по сроку хранения бакета.
    """
    new_index, to_write, to_delete, skipped = plan(found, index, root, titles=True)
    stale = (now - timedelta(days=DOC_REFRESH_DAYS)).isoformat()
    for rel, meta in new_index.items():
        at = (index.get(rel) or {}).get('at')
        if rel not in to_write and (not at or at < stale):
            to_write[rel] = found[rel].read_text(encoding='utf-8')
        meta['at'] = now.isoformat() if rel in to_write else at
    return new_index, to_write, to_delete, skipped


def _docs_s3():
    """(S3Hook, bucket) бакета логов: соединение и бакет — из настроек логирования."""
    from airflow.configuration import conf
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    base = conf.get('logging', 'remote_base_log_folder')
    return S3Hook(aws_conn_id=conf.get('logging', 'remote_log_conn_id')), base.split('://', 1)[1].split('/', 1)[0]


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
        'purge_docs': Param(
            False, type='boolean', title='Удалить сохранённые тексты',
            description='Разово удалить прежние тексты af_doc__* из Variables (в переменную не сохраняется).',
        ),
        'schedule': Param(
            SAVED.get('schedule', DEFAULT_SCHEDULE), type=['string', 'null'], title='Расписание',
            description='cron или пресет (@daily); пусто — только вручную. Применяется со следующего разбора',
        ),
        'save_params': Param(
            False, type='boolean', title='Сохранить параметры',
            description=f'Записать schedule в {PARAMS_VAR}: по нему пойдут и плановые запуски.',
        ),
    },
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    on_failure_callback=on_callback,
)
def tools_mcp_skills():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет schedule в переменную как значение по умолчанию."""
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
        purge = bool(context['params'].get('purge_docs'))
        index = Variable.get(DOC_INDEX, default_var={}, deserialize_json=True) or {}
        new_index, to_write, to_delete, skipped = docs_plan(
            find_docs(root), index, root, datetime.now(timezone.utc))

        hook, bucket = _docs_s3()
        for rel, text in to_write.items():
            hook.load_string(text, key=f'{DOCS_ROOT}/{rel}', bucket_name=bucket, replace=True)
        if to_delete:
            hook.delete_objects(bucket=bucket, keys=[f'{DOCS_ROOT}/{rel}' for rel in to_delete])
        purged = _purge_docs() if purge else 0
        # Оглавление — последним: страница верит ему и ссылаться оно должно на записанное
        if new_index != index:
            Variable.set(DOC_INDEX, new_index, serialize_json=True,
                         description="Оглавление документации дагов (Docs → DAG Docs): путь → sha256, размер, "
                                     "заголовок, at; тексты — docs/<путь> в бакете логов")

        title = f"DAG Docs: {len(new_index)}, записано в бакет {len(to_write)}, снято {len(to_delete)}"
        rows = [f"🧹 удалено af_doc__*: {purged}"] if purge else []
        rows += [f"✏️ `{r}`" for r in sorted(to_write)] + [f"🗑️ `{r}`" for r in to_delete]
        rows += [f"❌ {s}" for s in skipped]
        add_note("\n".join(rows) or "без изменений", context, level='DAG,task', title=title)
        if skipped:
            raise ValueError("Документы пропущены: " + "; ".join(skipped))
        return {'docs': len(new_index), 'written': sorted(to_write), 'deleted': to_delete, 'purged': purged}

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
