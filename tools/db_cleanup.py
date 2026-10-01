"""### 🧹 Очистка метадаты Airflow
*2026-10-01 22:37 MSK · v2.17 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Раз в сутки удаляет из метабазы записи старше `retention_days`: порциями, ребёнок раньше
родителя по внешним ключам. По галкам — VACUUM ANALYZE, переиндексация по одному индексу,
удаление остатков прерванного REINDEX. Таск `integrity` проверяет целостность метабазы
(плагин здоровья: находки → `health_warn` / `health_errors`).

Вакуум, переиндексация и удаление остатков (галки `vacuum`, `reindex`, `drop_leftovers`) —
только при админской учётке метабазы в Vault, иначе нет ни галок, ни тасков.

**Таски:** `params` → `drop_leftovers` → `clean` → `vacuum` → `reindex` → `report`, `integrity` →
`health_warn` / `health_errors`.

> `dry_run=False` по умолчанию — это реальное удаление.

Подробно: [tools/readme.md — db_cleanup](../../_plugin_dag_docs/?doc=tools/readme.md#db_cleanuppydb_cleanuppy)
"""

# Только то, что нужно на парсинге DAG (scheduler/dag-processor): декораторы,
# Param/TriggerRule для сигнатуры, datetime для start_date, cheap-stdlib.
# Всё runtime-only (модели, exceptions, session, settings, config_dict, pprint,
# unicodedata) импортируется внутри тасков/хелперов — грузится на воркере.
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text

from datetime import date, datetime, timedelta, timezone
import time
import logging

try:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, Feed, add_note, af_admin_available, ensure_pool, get_af_conn, health_tasks, on_callback, push_health, readable_size,
        saved_params, store_params_task, saved_schedule,
    )
except ImportError:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, Feed, add_note, af_admin_available, ensure_pool, get_af_conn, health_tasks, on_callback, push_health, readable_size,
        saved_params, store_params_task, saved_schedule,
    )

logger = logging.getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)


BATCH_SIZE = 50_000

# Дополнительные условия: не трогать строки ранов моложе cutoff.
# {p} — префикс таблицы: её имя ('task_instance.') или алиас 'base.'; пустым не бывает, см. _do_cleanup.
# execution_date ≤ start_date всегда, поэтому start_date < cutoff ⟹ execution_date < cutoff
# (безопасное добавление). Индекса по execution_date в AF 2.11 нет — только вторым полем
# в (dag_id, execution_date), — поэтому EXISTS находит ран по уникальному (dag_id, run_id)
# (у xcom — по dag_run.id), а дату проверяет у одной найденной строки.
_EXTRA_COND = {
    'dag_run': '{p}execution_date < :cutoff',
    'task_instance': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_instance_history': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_fail': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_reschedule': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    # dag_run_id — FK на dag_run.id; подзапрос идёт по первичному ключу dag_run
    'xcom': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.id = {p}dag_run_id AND _dr.execution_date < :cutoff)'
    ),
    # celery_taskmeta здесь нет намеренно: до v1.9 строка удалялась только при живом
    # task_instance с тем же external_executor_id. Колонка хранит id ТОЛЬКО последней
    # попытки, поэтому результаты всех ретраев, перезапусков и удалённых вручную ранов
    # осиротевали навсегда, а сам EXISTS шёл полным сканом task_instance — индекса по
    # external_executor_id нет ни у Airflow, ни в af_config.sql. Условие возраста ему
    # задаётся отдельно, см. _BASE_WHERE.
}

# Замена стандартного условия возраста `<колонка> < :cutoff` там, где его недостаточно.
# Подставляется вместо него, а не рядом: нужен OR, а _EXTRA_COND умеет только AND.
_BASE_WHERE = {
    # У celery_taskmeta date_done появляется ТОЛЬКО при переходе задачи в терминальное
    # состояние. У строки, застрявшей в STARTED (воркер умер, не закрыв задачу), его нет
    # никогда — а это ровно те строки, ради которых чистка и затевалась. Замер 18.09.2026:
    # из 314 820 строк дата стоит у всех SUCCESS и FAILURE и отсутствует у обеих STARTED;
    # на альфе таких накопилось 91 при 94 в карточке Health. То есть v1.9 с её
    # `date_done < cutoff` не удаляла их НИ РАЗУ — мой же комментарий был неверен.
    #
    # Возраст такой строки берём по id: он serial и растёт вместе с date_done (проверено —
    # min=1, max=314823, count=314823, порядок совпадает). Граница — МИНИМУМ id среди строк,
    # которые ещё НЕ вышли за срок: всё, что вставлено раньше самой ранней «свежей» строки,
    # заведомо старое.
    #
    # Первым заходом здесь стоял максимум id среди уже старых — и он оставлял сироту, которая
    # сама оказалась верхней в старом диапазоне: став сиротой, она выпала из множества «старых
    # с датой», по которому и считается максимум, и граница ушла ниже неё. Замер на стенде
    # (симуляция полного цикла удаления, четыре подставные сироты на разной высоте): у
    # максимума оставалась верхняя, у минимума не осталось ни одной, живые STARTED не тронуты
    # в обоих вариантах.
    #
    # Своя слабость есть и здесь: задача, начатая 40 дней назад и закончившаяся 5 дней назад,
    # имеет маленький id и свежий date_done — она утянет минимум вниз, и часть сирот доживёт
    # до следующего запуска. Это недоудаление, а не переудаление: осадок уйдёт, когда более
    # новые строки перешагнут cutoff. Выбор между «иногда позже» и «никогда» очевиден.
    #
    # ⚠️ Всё это держится на том, что celery_taskmeta есть в _PK_BATCH. Там батч — это
    # `ORDER BY id LIMIT n`, и строка без даты из него не выпадает. Убрать таблицу оттуда —
    # и включится батч по диапазону дат (`date_done >= :b_s AND date_done < :b_e`), который
    # припишется через AND и отсечёт ВСЕ строки с NULL: сироты снова перестанут удаляться,
    # молча. Подзапрос границы при этом пересчитывается на каждом батче, и это безвредно:
    # удаляем снизу вверх по id, а граница держится за самую раннюю свежую строку, которую
    # мы не трогаем вовсе.
    #
    # Нет ни одной свежей строки — подзапрос даёт NULL, сравнение ложно, не удаляется ничего.
    # Это и защищает живую STARTED-строку работающей задачи: у неё date_done пуст, и без
    # границы по id её снесло бы вместе с осадком.
    #
    # Трёхзначная логика тут работает на нас, и её не надо «чинить» через COALESCE: у живой
    # строки `date_done < :cutoff` даёт NULL, второй дизъюнкт — FALSE (id больше границы),
    # и всё выражение остаётся NULL. WHERE берёт только TRUE, поэтому DELETE её не трогает.
    # Проверено на стенде откатом: старые сироты удаляются, обе живые STARTED остаются.
    'celery_taskmeta': (
        '({col} < :cutoff OR ({col} IS NULL AND {p}id < '
        '(SELECT MIN(id) FROM main.celery_taskmeta WHERE date_done >= :cutoff)))'
    ),
}

# Каскад делает батч бессмысленным: у task_instance и task_reschedule FK на dag_run
# стоит ON DELETE CASCADE (af_config.sql:806,808), у task_instance_history — на
# task_instance (879). Порядок таблиц строим по FK (дети раньше родителей), но часть
# детей в чистку не входит вовсе (task_map, rendered_task_instance_fields, заметки) —
# их каскад остаётся, поэтому у dag_run батч дополнительно уменьшаем.
_BATCH_DIV = {'dag_run': 10}

# Таблицы без индекса на recency-колонке но с integer PK —
# удаляем через ORDER BY pk LIMIT batch_size чтобы использовать PK-индекс.
# Иначе каждый батч по диапазону дат — полный проход по таблице: у task_reschedule
# (start_date), task_fail (start_date) и job (latest_heartbeat) индекса по дате нет
# (у job — только вторым полем). id растёт вместе с датой, так что старые строки идут первыми.
# ponytail: task_instance и xcom без целочисленного PK остаются на батчах по датам —
# проход по таблице на батч; делить по dag_run.id, если ночная чистка начнёт не успевать.
_PK_BATCH = {
    'celery_tasksetmeta': 'id',
    'celery_taskmeta':    'id',
    'callback_request':   'id',
    'import_error':       'id',
    'task_reschedule':    'id',
    'task_fail':          'id',
    'job':                'id',
}

# Таблицы вне стандартного _cleanup_config с дополнительным safety-фильтром (opt-in через custom=True).
_CUSTOM_TABLES = {
    # Исходники DAG-файлов — нельзя трогать то, на что ссылается serialized_dag
    'dag_code': {
        'col': 'last_updated',
        'safe_where': (
            'NOT EXISTS (SELECT 1 FROM main.serialized_dag sd WHERE sd.fileloc_hash = dag_code.fileloc_hash)'
        ),
    },
    # Устаревший pickle-формат — нельзя трогать то, на что ссылается dag.pickle_id
    'dag_pickle': {
        'col': 'created_dttm',
        'safe_where': (
            'NOT EXISTS (SELECT 1 FROM main.dag d WHERE d.pickle_id = dag_pickle.id)'
        ),
    },
}


def _order_children_first(names, session):
    """Сортирует таблицы так, чтобы ребёнок чистился раньше родителя.

    Порядок Airflow (`config_dict`) алфавитный: dag_run пятым, task_instance
    тринадцатым, xcom последним. С ON DELETE CASCADE это значит, что батч в 50 000
    ранов тянет в одну транзакцию всех их детей — миллионы строк, то есть батчинг
    не работает ровно там, где он нужен. Связи берём из pg_constraint, а не списком:
    состав таблиц меняется с версией Airflow, а список молча устаревает.
    """
    edges = session.execute(text("""
        SELECT c.relname AS child, f.relname AS parent
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_class f ON f.oid = con.confrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE con.contype = 'f' AND n.nspname = 'main' AND c.relname <> f.relname
    """)).fetchall()

    children = {}
    for child, parent in edges:
        children.setdefault(parent, set()).add(child)

    ranks = {}

    def rank(tbl, seen=()):
        # Ранг = глубина дерева детей. Лист — 0, родитель всегда больше своих детей,
        # значит при сортировке по возрастанию дети идут первыми.
        if tbl in ranks:
            return ranks[tbl]
        if tbl in seen:          # цикл FK — не углубляемся, порядок тут не спасёт
            return 0
        kids = children.get(tbl, ())
        ranks[tbl] = 1 + max((rank(k, seen + (tbl,)) for k in kids), default=-1)
        return ranks[tbl]

    # sorted устойчив: внутри одного ранга остаётся алфавитный порядок Airflow
    return sorted(names, key=rank)


def _log_sql(sql, bind, msg="SQL"):
    """Логирует SQL с подставленными параметрами (упрощённо)."""
    try:
        from sqlalchemy.sql import text as sa_text
        if isinstance(sql, str):
            sql = sa_text(sql)
        # Берём сырой SQL с плейсхолдерами :cutoff/:b_s/:b_e/:lim.
        # Компиляция с literal_binds=True рендерит несвязанный :cutoff как NULL
        # ещё до подстановки ниже, поэтому её не используем.
        q = str(sql)
        # Подставляем значения
        for k, v in bind.items():
            if isinstance(v, (datetime, date)):
                v = f"'{v.isoformat()}'"
            elif isinstance(v, str):
                v = f"'{v}'"
            elif v is None:
                v = 'NULL'
            else:
                v = str(v)
            q = q.replace(f":{k}", v)
        logger.info(f"{msg}:\n{q}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось развернуть SQL ({e}): {sql} | Параметры: {bind}")


def db_stats(tables):
    """Снимок pg_stat_user_tables по таблицам схемы main: {table: (dead, last_vacuum)}.

    Статистика наполняется коллектором асинхронно (~500 мс), поэтому читать её
    сразу после VACUUM бессмысленно — снимок «после» снимаем с паузой.
    """
    from airflow import settings

    sql = text("""
        SELECT relname, n_dead_tup, GREATEST(last_vacuum, last_autovacuum)
        FROM pg_stat_user_tables
        WHERE schemaname = 'main' AND relname = ANY(:tbls)
    """)
    with settings.engine.connect() as conn:
        rows = conn.execute(sql, {'tbls': list(tables)}).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def db_vacuum(table, conn_id, full=False, timeout=3600):
    """VACUUM [FULL] ANALYZE по таблице схемы main под коннектом conn_id.

    VACUUM без FULL не уменьшает файл таблицы (страницы уходят в free space map),
    поэтому судить о его работе по размеру нельзя — ориентир n_dead_tup и last_vacuum.
    Без прав владельца VACUUM не падает, а молча пропускает таблицу с warning'ом —
    его и ловим, чтобы пропуск был виден.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    ts = time.time()
    mode = 'FULL ANALYZE' if full else 'ANALYZE'
    sql = f"VACUUM {mode} main.{table}"

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{timeout}s'")
            logger.info(f"🔧 VACUUM: {sql} (conn={conn_id})")
            del conn.notices[:]
            cur.execute(sql)
            notices = list(conn.notices)
    finally:
        conn.close()

    for n in notices:
        logger.info(f"📣 {table}: {n.strip()}")
    skipped = next((n for n in notices if 'skipping' in n.lower()), None)
    if skipped:
        raise AirflowSkipException(skipped.strip())
    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


def _fmt_ts(ts):
    """'HH:MM:SS' для отметок вакуума; '—' если статистики по таблице нет."""
    return ts.strftime('%H:%M:%S') if ts else '—'


# ── Целостность метабазы (таск integrity) ─────────────────────────────
# Только чтение каталога и статистики PG: без блокировок таблиц, секунды. Повод — сигма dev
# 01.10.2026: прерванный несколько раз REINDEX SCHEMA CONCURRENTLY оставил на dag_run 551 лишний
# индекс на 4 ГБ, каждая запись в dag_run обновляла их все, и шедулер с карточкой Health стояли.
# amcheck (bt_index_check) не берём: читает индексы целиком и требует CREATE EXTENSION.

INTEGRITY_TIMEOUT_MS = 30_000
# Остаток REINDEX CONCURRENTLY: _ccnew — недостроенная копия, _ccold — старый индекс после
# подмены. Повторные REINDEX SCHEMA перестраивают и остатки, отсюда цепочки _ccold1_ccold_ccnew
LEFTOVER_SQL = "c.relname ~ '_cc(new|old)[0-9]*$'"
LEFTOVER_SUFFIX = r'(_cc(?:new|old)\d*)+$'
# PG встаёт на защиту от переполнения счётчика транзакций у 2 млрд
XID_WARN, XID_ERROR = 1_000_000_000, 1_500_000_000
# job.id, log.id, celery_taskmeta.id в AF 2.11 — int4
SEQ_WARN, SEQ_ERROR = 0.7, 0.9
VACUUM_TABLES = ('dag_run', 'task_instance', 'job', 'log', 'xcom', 'celery_taskmeta')
DEAD_SHARE, DEAD_MIN = 0.2, 100_000
LONG_TX = '1 hour'
# Возраст горизонта вакуума в транзакциях. Сигма dev 01.10.2026: физический слот реплики держал
# xmin на 2,1 млн, вакуум не убирал старые версии строк task_instance, и проверка пула шла
# 1,5 с вместо 1 мс — шаг шедулера 25–35 с вместо 3 с
XMIN_WARN, XMIN_ERROR = 100_000, 1_000_000
# reindex: индекс, похудевший меньше, в итоговую заметку отдельной строкой не попадает
REINDEX_NOTE_MIN = 1024 ** 2
# Логические слоты (CDC чужих систем читают метабазу; сигма dev 01.10.2026 — три source_*) держат
# только catalog_xmin: вакуум каталога, не task_instance. Их риск — WAL, который мастер копит,
# пока потребитель стоит. ponytail: порог в ГБ без знания размера диска; поднять, если шумит
LOGICAL_WAL_WARN_GB, LOGICAL_WAL_ERROR_GB = 5, 20
CHECK_ICON = {'healthy': '✅', 'warn': '⚠️', 'error': '❌'}
def _catalog(sql, **bind):
    """SELECT по каталогу метабазы с потолком на запрос."""
    from airflow.utils.session import create_session

    with create_session() as session:
        session.execute(text(f"SET LOCAL statement_timeout = {INTEGRITY_TIMEOUT_MS}"))
        return [dict(r) for r in session.execute(text(sql), bind).mappings()]


def _run_check(fn):
    """Одна проверка; упала сама — error с причиной, таймаут метабазы — warn."""
    ts = time.time()
    try:
        result = fn()
    except Exception as exc:
        logger.warning(f"{fn.__name__}: проверка упала", exc_info=True)
        if type(getattr(exc, 'orig', None)).__name__ == 'QueryCanceled':
            result = {'status': 'warn',
                      'summary': f'метабаза не ответила за {INTEGRITY_TIMEOUT_MS // 1000} с'}
        else:
            result = {'status': 'error', 'summary': f'проверка упала: {type(exc).__name__}: {str(exc)[:200]}'}
    result['sec'] = round(time.time() - ts, 2)
    return result


def check_indexes():
    """Остатки прерванного REINDEX — error; невалидные и дубли — warn."""
    rows = _catalog(f"""
        SELECT t.relname AS tbl, c.relname AS idx, i.indisvalid AND i.indisready AS valid,
               pg_relation_size(c.oid) AS bytes, {LEFTOVER_SQL} AS leftover
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main'
    """)
    leftovers, by_table = [r for r in rows if r['leftover']], {}
    for r in leftovers:
        cnt, size = by_table.get(r['tbl'], (0, 0))
        by_table[r['tbl']] = (cnt + 1, size + r['bytes'])
    invalid = [f"{r['tbl']}.{r['idx']}" for r in rows if not r['valid'] and not r['leftover']]
    # Дубли — одинаковые колонки, классы операторов, выражения и условие; остатки не в счёт
    dups = _catalog(f"""
        SELECT t.relname AS tbl, string_agg(c.relname, ', ' ORDER BY c.relname) AS idx
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND NOT ({LEFTOVER_SQL})
        GROUP BY t.relname, i.indrelid, i.indkey::text, i.indclass::text,
                 coalesce(pg_get_expr(i.indexprs, i.indrelid), ''),
                 coalesce(pg_get_expr(i.indpred, i.indrelid), '')
        HAVING count(*) > 1
    """)
    parts = []
    if by_table:
        parts.append('остатки прерванного REINDEX: ' + ', '.join(
            f'{t} {n} шт. ({readable_size(b)})' for t, (n, b) in sorted(by_table.items(), key=lambda x: -x[1][1])))
    if invalid:
        parts.append(f"невалидные: {', '.join(invalid[:10])}")
    if dups:
        parts.append('дубли: ' + '; '.join(f"{d['tbl']}: {d['idx']}" for d in dups[:10]))
    status = 'error' if by_table else 'warn' if invalid or dups else 'healthy'
    return {'status': status,
            'summary': ' · '.join(parts) or f'{len(rows)} индексов, остатков, невалидных и дублей нет',
            'leftovers': [r['idx'] for r in leftovers], 'invalid': invalid,
            'duplicates': [d['idx'] for d in dups]}


def check_wraparound():
    """Возраст самой старой незамороженной транзакции базы и таблиц main."""
    db_age = _catalog("SELECT age(datfrozenxid) AS age FROM pg_database WHERE datname = current_database()")[0]['age']
    top = _catalog("""
        SELECT c.relname AS tbl, age(c.relfrozenxid) AS age
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND c.relkind IN ('r', 't', 'm')
        ORDER BY age(c.relfrozenxid) DESC LIMIT 3
    """)
    status = 'error' if db_age > XID_ERROR else 'warn' if db_age > XID_WARN else 'healthy'
    return {'status': status,
            'summary': f"возраст базы {db_age / 1e6:.0f} млн из 2000 млн; старше всех: "
                       + ', '.join(f"{r['tbl']} {r['age'] / 1e6:.0f} млн" for r in top),
            'age': db_age}


def check_sequences():
    """Доля израсходованного у последовательностей main: int4 кончается на 2,1 млрд."""
    rows = _catalog("""
        SELECT sequencename AS seq, last_value, max_value
        FROM pg_sequences WHERE schemaname = 'main' AND last_value IS NOT NULL
    """)
    used = sorted(((r['last_value'] / r['max_value'], r['seq']) for r in rows), reverse=True)
    worst = used[0][0] if used else 0
    status = 'error' if worst > SEQ_ERROR else 'warn' if worst > SEQ_WARN else 'healthy'
    return {'status': status,
            'summary': (f"{len(rows)} последовательностей, больше всех: "
                        + ', '.join(f'{s} {share:.1%}' for share, s in used[:3])) if used
                       else 'последовательностей с last_value нет (или нет прав их видеть)'}


def check_vacuum():
    """Мёртвые строки ключевых таблиц: автовакуум не успевает."""
    rows = _catalog("""
        SELECT relname AS tbl, n_live_tup AS live, n_dead_tup AS dead,
               greatest(last_vacuum, last_autovacuum) AS last_vac
        FROM pg_stat_user_tables WHERE schemaname = 'main' AND relname = ANY(:t)
    """, t=list(VACUUM_TABLES))
    bad = [r for r in rows if r['dead'] > DEAD_MIN and r['dead'] > DEAD_SHARE * ((r['live'] or 0) + r['dead'])]
    return {'status': 'warn' if bad else 'healthy',
            'summary': ('мёртвых строк много: ' + ', '.join(
                f"{r['tbl']} {readable_size(r['dead'], base=1000)} из "
                f"{readable_size(r['live'] + r['dead'], base=1000)}, вакуум {r['last_vac'] or 'не было'}"
                for r in bad)) if bad else f'{len(rows)} таблиц, автовакуум успевает'}


def check_long_tx():
    """Транзакции старше часа держат вакуум всей базы. Чужие сессии без pg_read_all_stats не видны."""
    rows = _catalog(f"""
        SELECT pid, usename, state, date_trunc('second', now() - xact_start) AS dur, left(query, 80) AS q
        FROM pg_stat_activity
        WHERE datname = current_database() AND backend_type = 'client backend'
          AND xact_start < now() - interval '{LONG_TX}' AND pid <> pg_backend_pid()
        ORDER BY xact_start LIMIT 5
    """)
    return {'status': 'warn' if rows else 'healthy',
            'summary': ('транзакции дольше часа: ' + '; '.join(
                f"pid {r['pid']} {r['usename']} {r['state']} {r['dur']}: {r['q']}" for r in rows))
                       if rows else 'транзакций дольше часа нет'}


def check_xmin_horizon():
    """Кто держит горизонт вакуума таблиц: физические слоты реплик (с hot_standby_feedback — и
    запросы на реплике) и сессии самой базы. Логические слоты — в check_logical_slots."""
    rows = _catalog("""
        SELECT 'слот ' || slot_name AS who, greatest(age(xmin), age(catalog_xmin)) AS age
        FROM pg_replication_slots
        WHERE slot_type = 'physical' AND (xmin IS NOT NULL OR catalog_xmin IS NOT NULL)
        UNION ALL
        SELECT 'pid ' || pid || coalesce(' ' || nullif(application_name, ''), ''), age(backend_xmin)
        FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid()
        ORDER BY 2 DESC LIMIT 3
    """)
    worst = rows[0]['age'] if rows else 0
    status = 'error' if worst > XMIN_ERROR else 'warn' if worst > XMIN_WARN else 'healthy'
    return {'status': status,
            'summary': ('старше всех: ' + ', '.join(f"{r['who']} {r['age']:,}".replace(',', ' ') for r in rows)
                        + (' транзакций — вакуум не убирает версии строк новее, индексы метабазы тяжелеют'
                           if status != 'healthy' else ' транзакций'))
                       if rows else 'горизонт никто не держит'}



def check_logical_slots():
    """Логические слоты: сколько WAL держат на мастере и стоит ли потребитель."""
    rows = _catalog("""
        SELECT slot_name, active, age(catalog_xmin) AS age,
               pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()
                                    ELSE pg_current_wal_lsn() END, restart_lsn) AS wal
        FROM pg_replication_slots WHERE slot_type = 'logical'
        ORDER BY wal DESC NULLS LAST
    """)
    if not rows:
        return {'status': 'healthy', 'summary': 'логических слотов нет'}
    gb = 1024 ** 3
    worst = max(r['wal'] or 0 for r in rows) / gb
    idle = [r['slot_name'] for r in rows if not r['active']]
    status = ('error' if worst > LOGICAL_WAL_ERROR_GB
              else 'warn' if worst > LOGICAL_WAL_WARN_GB or idle else 'healthy')
    parts = [f"{r['slot_name']}{'' if r['active'] else ' (не подключён)'}: WAL {readable_size(r['wal'] or 0)}, "
             f"catalog_xmin {format(r['age'] or 0, ',').replace(',', ' ')}" for r in rows[:3]]
    return {'status': status,
            'summary': '; '.join(parts) + (' — мастер копит WAL, пока потребитель не дочитает: к владельцу слота / DBA'
                                           if status != 'healthy' else '')}


def check_constraints():
    """Ограничения, заведённые NOT VALID и не проверенные: данные могут их нарушать."""
    rows = _catalog("""
        SELECT conrelid::regclass::text AS tbl, conname
        FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE n.nspname = 'main' AND NOT c.convalidated
    """)
    return {'status': 'warn' if rows else 'healthy',
            'summary': ('не проверены: ' + ', '.join(f"{r['tbl']}.{r['conname']}" for r in rows))
                       if rows else 'все ограничения проверены'}


INTEGRITY_CHECKS = (check_indexes, check_wraparound, check_sequences, check_vacuum, check_long_tx, check_xmin_horizon,
                    check_logical_slots, check_constraints)


def owner_problem(cur, tables=None):
    """Чего не хватает учётке коннекта для работы с таблицами main; None — всё в порядке.

    Ключи в Vault не говорят о правах: VACUUM, ANALYZE и REINDEX не владельца PG 16
    пропускает с WARNING «permission denied … skipping it» без ошибки (проверено на стенде
    01.10.2026 временной ролью), поэтому права проверяются до первой команды.
    """
    cur.execute("""
        SELECT current_user, coalesce((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false),
               coalesce(array_agg(c.relname ORDER BY c.relname)
                        FILTER (WHERE NOT pg_has_role(current_user, c.relowner, 'MEMBER')), '{}')
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND c.relkind = 'r' AND (%(t)s::text[] IS NULL OR c.relname = ANY(%(t)s))
    """, {'t': list(tables) if tables else None})
    user, superuser, foreign = cur.fetchone()
    if superuser or not foreign:
        return None
    return f"учётка {user} не владеет таблицами: {', '.join(foreign[:10])}" + (' и другими' if len(foreign) > 10 else '')


def af_owner_problem(tables=None):
    """owner_problem() на коннекте get_af_conn() — том, под которым пойдёт работа."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    conn = PostgresHook(postgres_conn_id=get_af_conn()).get_conn()
    try:
        with conn.cursor() as cur:
            return owner_problem(cur, tables)
    finally:
        conn.close()


# Переиндексация: каждый индекс своей командой, от меньших к большим. Обрыв посреди
# REINDEX SCHEMA CONCURRENTLY на сигме dev (01.10.2026) оставил 551 остаток на dag_run; по
# одному индексу обрыв оставляет одну копию, и её убираем сразу. Порядок по размеру: за
# бюджет успевает больше индексов, а недоделанными остаются немногие крупные — их видно
REINDEX_BUDGET_SEC = 3 * 3600  # execution_timeout 4 ч: после бюджета новые не начинаем
REINDEX_STATEMENT_TIMEOUT = '1h'


def reindex_one_by_one(budget_sec=REINDEX_BUDGET_SEC, on_step=None):
    """REINDEX INDEX CONCURRENTLY по каждому индексу main, кроме остатков.

    Returns: (done, failed, left) или строка — причина не начинать (нет прав).
    Отказ индекса убирает его недостроенную копию (*_ccnew) сразу же, а не вышло — ещё раз в
    конце; остальные индексы той же таблицы после отказа не начинаются (в left).
    """
    import re
    from psycopg2 import sql as psql
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    rows = _catalog(f"""
        SELECT t.relname AS tbl, c.relname AS idx, pg_relation_size(c.oid) AS bytes
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND NOT ({LEFTOVER_SQL})
        ORDER BY pg_relation_size(c.oid), c.relname
    """)
    size_sql = ("SELECT c.relname, pg_relation_size(c.oid) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'main' AND c.relkind = 'i' AND c.relname LIKE %s")
    done, failed, left = [], [], []
    started = time.monotonic()

    def drop(cur, names):
        """Удалить копии; вернуть те, что удалить не вышло."""
        kept = []
        for name in names:
            try:
                cur.execute(psql.SQL('DROP INDEX CONCURRENTLY IF EXISTS main.{}').format(psql.Identifier(name)))
            except Exception as exc:
                logger.warning(f"{name}: копию пока не удалить: {exc}")
                kept.append(name)
        return kept

    conn = PostgresHook(postgres_conn_id=get_af_conn()).get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            problem = owner_problem(cur, sorted({r['tbl'] for r in rows}))
            if problem:
                return problem
            cur.execute("SET lock_timeout = '30s'")
            cur.execute(f"SET statement_timeout = '{REINDEX_STATEMENT_TIMEOUT}'")
            blocked = set()  # таблицы с отказом: следующий индекс там оставил бы ещё одну копию
            for r in rows:
                if time.monotonic() - started > budget_sec or r['tbl'] in blocked:
                    left.append(r)
                    continue
                ts = time.monotonic()
                try:
                    cur.execute(psql.SQL('REINDEX INDEX CONCURRENTLY main.{}').format(psql.Identifier(r['idx'])))
                    cur.execute(size_sql, (r['idx'],))
                    after = dict(cur.fetchall()).get(r['idx'])
                    done.append({**r, 'after': after, 'sec': round(time.monotonic() - ts, 1)})
                    if on_step:
                        on_step(done[-1], None)
                except Exception as exc:
                    # Своя недостроенная копия: <idx>_ccnew[N]. ponytail: имя длиннее 63 символов
                    # PG усекает — такую копию не узнаем, её найдёт integrity
                    cur.execute(size_sql, (r['idx'] + '\\_ccnew%',))
                    mine = [n for n, _ in cur.fetchall() if re.fullmatch(re.escape(r['idx']) + r'_ccnew\d*', n)]
                    kept = drop(cur, mine)
                    failed.append({**r, 'error': str(exc).strip()[:160], 'kept': kept,
                                   'dropped': [n for n in mine if n not in kept]})
                    blocked.add(r['tbl'])
                    logger.warning(f"❌ {r['idx']}: {exc}")
                    if on_step:
                        on_step(failed[-1], failed[-1]['error'])
            # Копию, которую не дал удалить тот же блокирующий (стенд: DROP CONCURRENTLY ждёт ту же
            # транзакцию и падает по lock_timeout), пробуем ещё раз в конце — блокировка могла уйти
            for r in failed:
                if r['kept']:
                    still = drop(cur, r['kept'])
                    r['dropped'] += [n for n in r['kept'] if n not in still]
                    r['kept'] = still
    finally:
        conn.close()
    logger.info(f"🔁 переиндексировано {len(done)}, отказов {len(failed)}, не успели {len(left)}")
    return done, failed, left


# Остатки удаляются одной командой без CONCURRENTLY: каталог и файлы, секунды на все. Ей нужна
# эксклюзивная блокировка таблиц, и пока она ждёт, остальные запросы к ним стоят за ней — поэтому
# ждём недолго и повторяем. CONCURRENTLY по одному на сигме dev (01.10.2026) шёл 2 мин на индекс:
# каждый дважды ждёт конца всех транзакций по dag_run, 551 остаток — это 18 ч
DROP_LOCK_TIMEOUT = '3s'
DROP_TRIES, DROP_PAUSE_SEC = 20, 15


def drop_leftovers(names):
    """DROP INDEX остатков REINDEX одной командой — только тех, у кого исходный индекс есть и валиден.

    Под коннектом владельца (get_af_conn): у штатного пользователя Airflow прав нет. Не взяли
    блокировку за ``DROP_LOCK_TIMEOUT`` — пауза и повтор, до ``DROP_TRIES`` раз. Команда упала по
    другой причине (индекс держит ограничение) — по одному, чтобы назвать виновного; это строка
    в итоге, а не падение.
    """
    import re
    from psycopg2 import errors as pg_errors, sql as psql
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    valid = {r['idx'] for r in _catalog("""
        SELECT c.relname AS idx FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND i.indisvalid AND i.indisready
    """)}
    todo, done, kept = [], [], []
    for name in names:
        base = re.sub(LEFTOVER_SUFFIX, '', name)
        if base == name or base not in valid:
            kept.append(f'{name}: исходного {base} нет или он невалиден')
        else:
            todo.append(name)
    if not todo:
        return done, kept
    conn = PostgresHook(postgres_conn_id=get_af_conn()).get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            problem = owner_problem(cur)
            if problem:
                return [], kept + [f'не выполнено: {problem}']
            cur.execute(f"SET lock_timeout = '{DROP_LOCK_TIMEOUT}'")
            cur.execute("SET statement_timeout = '10min'")

            def drop(batch):
                """None — удалены; иначе причина. lock — не взяли блокировку ни разу."""
                stmt = psql.SQL('DROP INDEX IF EXISTS {}').format(
                    psql.SQL(', ').join(psql.Identifier('main', n) for n in batch))
                for attempt in range(1, DROP_TRIES + 1):
                    try:
                        cur.execute(stmt)
                        return None
                    except pg_errors.LockNotAvailable:
                        logger.info(f"🧹 {len(batch)} шт.: блокировка не взята за {DROP_LOCK_TIMEOUT}, "
                                    f"попытка {attempt}/{DROP_TRIES}")
                        if attempt < DROP_TRIES:
                            time.sleep(DROP_PAUSE_SEC)
                    except Exception as exc:
                        return str(exc).strip()[:120]
                return f'lock: блокировку не взяли за {DROP_TRIES} попыток по {DROP_LOCK_TIMEOUT}'

            started = time.monotonic()
            error = drop(todo)
            if error is None:
                done = todo
            elif error.startswith('lock:'):
                kept += [f'{n}: {error[6:]}' for n in todo]
            else:
                logger.warning(f"🧹 одной командой не вышло ({error}) — по одному")
                for name in todo:
                    error = drop([name])
                    if error is None:
                        done.append(name)
                    else:
                        kept.append(f"{name}: {error.removeprefix('lock: ')}")
            logger.info(f"🧹 остатков удалено {len(done)} за {time.monotonic() - started:.1f} с")
    finally:
        conn.close()
    logger.info(f"🧹 остатков удалено {len(done)}, оставлено {len(kept)}")
    return done, kept


# Значения по умолчанию для формы запуска: код задаёт запасной вариант, переменная —
# рабочий. Пишет переменную только запуск с save_params=True, см. таск params.
PARAMS_VAR = 'tools_db_cleanup_params'
SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


# 05:00 MSK: cron в зоне start_date, как у соседних дагов (до 29.09.2026 — UTC, '0 2 * * *')
DEFAULT_SCHEDULE = '0 5 * * *'
MSK = timezone(timedelta(hours=3))
ONE_SHOT = ('drop_leftovers', 'reindex')
# Строка хода удаления остатков в заметке integrity — не чаще (как NOTE_EVERY_SEC у clean)
# Админская учётка метабазы в Vault (get_af_conn). Без неё VACUUM, REINDEX и удаление индексов
# невозможны — ни задач, ни параметров для них не создаём: спрашивать о том, что даг сделать
# не может, незачем. Права самой учётки проверяет таск перед работой (owner_problem)
ADMIN = af_admin_available()
# Плагин здоровья (тег health): сутки плюс два часа на опоздание ночного прогона
REPORT_TTL_SEC = 26 * 3600



params = {
    'retention_days': _param(
        'retention_days', 180,
        type='integer',
        minimum=30,
        description='Хранить записи не старше N дней (минимум 30)',
    ),
    'dry_run': _param(
        'dry_run', False,
        type='boolean',
        description='True — только подсчёт в clean, False — реальное удаление. На vacuum, reindex и drop_leftovers не влияет',
    ),
    'custom': _param(
        'custom', False,
        type='boolean',
        description='True — включить dag_code и dag_pickle, False — только стандартные таблицы',
    ),
    'batch_size': _param(
        'batch_size', BATCH_SIZE,
        type='integer',
        minimum=1000,
        description='Максимальный размер порции при удалении (строк)',
    ),
    'lock_timeout': _param(
        'lock_timeout', '10min',
        type='string',
        description='Таймаут ожидания блокировки (например: 10min, 30s)',
    ),
    'schedule': _param(
        'schedule', DEFAULT_SCHEDULE,
        type='string',
        description='Расписание: cron или пресет @daily; пусто — только вручную. Применяется со следующего парсинга',
    ),
    # Разовое действие, а не настройка: в переменную не сохраняется и берётся всегда из кода
    'save_params': Param(
        False,
        type='boolean',
        description='True — сохранить параметры этого запуска как значения по умолчанию',
    ),
}
if ADMIN:
    params.update({
        'vacuum': _param(
            'vacuum', True,
            type='boolean',
            description='True — VACUUM ANALYZE очищенных таблиц, False — пропустить',
        ),
        # Разовые: из кода, не из SAVED, и в ONE_SHOT — ключ reindex: true, оставшийся в
        # Variable от v1.10, их не включит (так 18.09.2026 «выключенный» реиндекс запускался)
        'reindex': Param(
            False,
            type='boolean',
            description='True — перестроить индексы main по одному (REINDEX INDEX CONCURRENTLY), '
                        'от меньших к большим. Разовая: в Variable не сохраняется',
        ),
        'drop_leftovers': Param(
            False,
            type='boolean',
            description='True — удалить остатки прерванного REINDEX (индексы *_ccnew/*_ccold), '
                        'у которых исходный индекс есть и валиден. Разовая: в Variable не сохраняется',
        ),
    })


@dag(
    doc_md=__doc__,
    description='Чистка метабазы Airflow по сроку хранения; вакуум, переиндексация и проверки целостности по галкам',
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Иначе вес по потомкам (у clean — 7) против сотен у больших дагов: планировщик берёт из
        # scheduled верхушку по весу и заканчивает цикл, как только в ней нашлось что запустить
        # (scheduler_job_runner, is_done), и до чистильщика очередь не доходит. Сигма dev
        # 01.10.2026: clean полчаса в scheduled при пустом пуле. Как у соседей по пулу — 900
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # Потолок от зависания на блокировке, а не от медленной чистки: VACUUM по таблице
        # сам ограничен часом (db_vacuum), удаление идёт порциями
        'execution_timeout': timedelta(hours=4),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2025, 8, 7, tzinfo=MSK),
    # Тег health — роль: таск integrity даёт вердикт, get_system_health читает отчёт
    tags=['DataTools', 'tools', 'clean', 'health'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR),
    on_failure_callback=on_callback,
    params=params,
)
def tools_db_cleanup():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию."""
        return store_params_task(PARAMS_VAR, SAVED, context, one_shot=ONE_SHOT)

    # NONE_FAILED, а не дефолтный ALL_SUCCESS: params штатно пропускает себя при
    # save_params=False, а пропуск апстрима по ALL_SUCCESS утягивает в skip всю цепочку
    @task(task_id='clean', trigger_rule=TriggerRule.NONE_FAILED)
    def clean(**context):
        """Удаляет записи метабазы старше `retention_days` (при `dry_run` только считает); отдаёт список таблиц для vacuum."""
        from airflow.exceptions import AirflowFailException
        from airflow.utils.db_cleanup import config_dict as _cleanup_config
        from airflow.utils.session import create_session

        p = context['params']
        retention_days = p['retention_days']
        if retention_days < 30:
            raise AirflowFailException(f'retention_days={retention_days} меньше минимума (30)')

        dry_run = p['dry_run']
        batch_size = p.get('batch_size', BATCH_SIZE)
        lock_timeout = p.get('lock_timeout', '10min')
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        def _fmt_date(d):
            return str(d)[:10] if d else '—'

        def _idx_label(tbl, col, session):
            """✅ прямой индекс / ↗ косвенный / 🔑 PK-батч / ❌ seq scan.

            Прямой — колонка первая в индексе: вторым полем (job_type, latest_heartbeat) диапазон
            по дате не ищется."""
            if col:
                n = session.execute(text("""
                    SELECT COUNT(*) FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace ns ON ns.oid = c.relnamespace
                    WHERE ns.nspname = 'main' AND c.relname = :tbl AND a.attname = :col
                """), {'tbl': tbl, 'col': col}).scalar()
                if n:
                    return '✅'
            if tbl in _PK_BATCH:
                return '🔑'
            if tbl in _EXTRA_COND:
                return '↗'
            return '❌'

        def _do_cleanup(tbl, session, on_batch=None):
            session.execute(text(f"SET lock_timeout = '{lock_timeout}'"))
            t = f'main.{tbl}'
            bind = {'cutoff': cutoff}
            logger.info(f"⚙️ Параметры очистки: retention_days={retention_days}, cutoff={cutoff}, dry_run={dry_run}, batch_size={batch_size}")

            if tbl in _CUSTOM_TABLES:
                # Таблицы вне стандартного Airflow cleanup — простой WHERE + safety-фильтр
                custom = _CUSTOM_TABLES[tbl]
                col = custom['col']
                idx = _idx_label(tbl, col, session)
                p = f'{tbl}.'
                base_where = f"{col} < :cutoff AND {custom['safe_where']}"
                count_sql = text(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {t} WHERE {base_where}")
                def make_delete(batch_extra=''):
                    w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                    return text(f"DELETE FROM {t} WHERE {w}")
            else:
                cfg = _cleanup_config[tbl]
                col = str(cfg.recency_column_name)
                idx = _idx_label(tbl, col, session)

                if cfg.keep_last and cfg.keep_last_group_by:
                    p = 'base.'
                    grp = cfg.keep_last_group_by[0]
                    keep_sub = (
                        f"SELECT {grp}, MAX({col}) AS _max FROM {t} "
                        f"WHERE external_trigger = false GROUP BY {grp}"
                    )
                    jc = f"base.{grp} = _l.{grp} AND base.{col} = _l._max"
                    base_where = f"base.{col} < :cutoff AND _l._max IS NULL"
                    from_clause = f"{t} base LEFT JOIN ({keep_sub}) _l ON {jc}"
                else:
                    # Имя таблицы, а не пустой префикс: неквалифицированный dag_id внутри
                    # EXISTS по dag_run PostgreSQL берёт у самого dag_run, и условие
                    # сравнивало поле с собой — пропускало всё (стенд, 27.09.2026:
                    # task_reschedule 231 669 строк вместо 188 028)
                    p = f'{tbl}.'
                    base_where = _BASE_WHERE.get(tbl, '{col} < :cutoff').format(col=col, p=p)
                    from_clause = t

                extra_cond = _EXTRA_COND.get(tbl, '').format(p=p)
                if extra_cond:
                    base_where += f' AND {extra_cond}'

                if cfg.keep_last and cfg.keep_last_group_by:
                    count_sql = text(
                        f"SELECT COUNT(*), MIN(base.{col}), MAX(base.{col}) "
                        f"FROM {from_clause} WHERE {base_where}"
                    )
                    def make_delete(batch_extra=''):
                        w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                        return text(f"DELETE FROM {t} WHERE id IN (SELECT base.id FROM {from_clause} WHERE {w})")
                else:
                    count_sql = text(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {t} WHERE {base_where}")
                    def make_delete(batch_extra=''):
                        w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                        return text(f"DELETE FROM {t} WHERE {w}")

            _log_sql(count_sql, bind, f"📊 COUNT {tbl}")
            row = session.execute(count_sql, bind).fetchone()
            count, min_date, max_date = row[0] or 0, row[1], row[2]

            batches = 0
            if count and not dry_run:
                tbl_batch = max(1000, batch_size // _BATCH_DIV.get(tbl, 1))
                n_batches = (count + tbl_batch - 1) // tbl_batch
                pk = _PK_BATCH.get(tbl)
                if pk and n_batches > 1:
                    # Нет индекса на recency-колонке — используем PK-индекс:
                    # DELETE WHERE pk IN (SELECT pk WHERE ... ORDER BY pk LIMIT batch_size)
                    pk_delete = text(
                        f"DELETE FROM {t} WHERE {pk} IN"
                        f" (SELECT {pk} FROM {t} WHERE {base_where}"
                        f" ORDER BY {pk} LIMIT :lim)"
                    )
                    while True:
                        _log_sql(pk_delete, {**bind, 'lim': tbl_batch}, f"🗑️ DELETE {tbl}")
                        res = session.execute(pk_delete, {**bind, 'lim': tbl_batch})
                        session.commit()
                        if res.rowcount == 0:
                            break
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                elif n_batches > 1 and min_date is not None:
                    # min_date из БД (timestamptz → aware). Приводим к aware UTC на
                    # случай timestamp-without-tz, чтобы вычитание с cutoff не падало.
                    start = min_date if min_date.tzinfo else min_date.replace(tzinfo=timezone.utc)
                    diff = (cutoff - start).total_seconds()
                    step = diff / n_batches
                    for j in range(n_batches):
                        b_s = start + timedelta(seconds=step * j)
                        b_e = cutoff if j == n_batches - 1 else start + timedelta(seconds=step * (j + 1))
                        batch_extra = f"{p}{col} >= :b_s AND {p}{col} < :b_e"
                        _log_sql(make_delete(batch_extra), {**bind, 'b_s': b_s, 'b_e': b_e}, f"🗑️ DELETE {tbl}")
                        session.execute(make_delete(batch_extra), {**bind, 'b_s': b_s, 'b_e': b_e})
                        session.commit()
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                else:
                    _log_sql(make_delete(), bind, f"🗑️ DELETE {tbl}")
                    session.execute(make_delete(), bind)
                    session.commit()
                    batches = 1

            return {'count': count, 'min_date': min_date, 'max_date': max_date,
                    'idx': idx, 'batches': batches}

        custom = p.get('custom', False)
        table_names = list(_cleanup_config.keys()) + (list(_CUSTOM_TABLES.keys()) if custom else [])
        try:
            with create_session() as session:
                table_names = _order_children_first(table_names, session)
            logger.info(f"📐 Порядок очистки (дети раньше родителей): {', '.join(table_names)}")
        except Exception as e:
            # Не смогли прочитать связи — чистим в порядке Airflow: медленнее, но не хуже прежнего
            logger.warning(f"⚠️ Порядок по FK не построен ({e}), идём алфавитным порядком Airflow")
        results = {}
        mode = '🔍 dry_run' if dry_run else '🗑️ удалено'
        _ts_total = time.time()
        n = len(table_names)
        # Заметка дописывается сверху строкой на таблицу, а не переписывается таблицей целиком:
        # свежее — наверху, ход чистки виден, итоговая таблица ложится сверху в конце.
        # Внутри долгой таблицы — строка хода не чаще раза в NOTE_EVERY_SEC, и она заменяет
        # прошлую строку хода той же таблицы: порций бывают сотни
        NOTE_EVERY_SEC = 300
        last_note = {'at': time.time(), 'progress': False}
        feed = Feed(context, f'🗑️ clean ({mode}, {retention_days}d)')

        for i, tbl in enumerate(table_names, 1):
            _ts = time.time()

            def _on_batch(done, total, count, min_date, idx, _tbl=tbl, _i=i):
                if time.time() - last_note['at'] < NOTE_EVERY_SEC:
                    return
                last_note['at'] = time.time()
                feed.line(f"⏳ {_tbl} ({_i}/{n}): порция {done}/{total}, {readable_size(count, base=1000)} строк, "
                          f"{round(time.time() - _ts, 1)} с", replace_last=last_note['progress'])
                last_note['progress'] = True

            try:
                with create_session() as session:
                    info = _do_cleanup(tbl, session, on_batch=_on_batch)
            except Exception as e:
                logger.warning(f"⚠️ {tbl}: {e}")
                results[tbl] = {'count': 0, 'min_date': None, 'idx': '⚠️',
                                 'duration': str(e)[:40], 'batches': 0}
                feed.line(f"⚠️ {tbl} ({i}/{n}): {str(e)[:120]}", replace_last=last_note['progress'])
                last_note.update(at=time.time(), progress=False)
                continue
            info['duration'] = round(time.time() - _ts, 2)
            results[tbl] = info
            logger.info(
                f"🔎 {tbl}: {info['count']} rows "
                f"[{_fmt_date(info['min_date'])}…{_fmt_date(info['max_date'])}] "
                f"idx={info['idx']} batches={info['batches']} {info['duration']}s"
            )

            feed.line(f"✅ {tbl} ({i}/{n}): {readable_size(info['count'], base=1000)} строк, {info['duration']} с",
                      replace_last=last_note['progress'])
            last_note.update(at=time.time(), progress=False)

        duration = round(time.time() - _ts_total, 2)

        if results:
            total = sum(r['count'] for r in results.values())
            # В итоге — только таблицы, где что-то было: пустые не интересны
            feed.done(f'{mode} {readable_size(total, base=1000)} строк | cutoff: {cutoff.strftime("%Y-%m-%d")}'
                      f' ⏱ {duration}s',
                      keep=[ln for ln in feed.lines if not ln.startswith('✅') or ': 0 строк' not in ln])
            add_note(
                f'{mode} {readable_size(total, base=1000)} строк | cutoff: {cutoff.strftime("%Y-%m-%d")}'
                f' ⏱ {duration}s',
                context=context, level='DAG', title='🗑️ clean',
            )
        else:
            add_note(
                f'{mode} | cutoff: {cutoff.strftime("%Y-%m-%d")} ⏱ {duration}s',
                context=context, level='DAG,Task', title='🗑️ clean',
            )

        return list(results.keys())

    @task(task_id='vacuum', trigger_rule=TriggerRule.ALL_DONE)
    def vacuum(**context):
        """VACUUM ANALYZE таблиц, которые чистил `clean`, под админской учёткой."""
        from airflow.exceptions import AirflowSkipException

        p = context['params']
        if not p.get('vacuum', True):
            raise AirflowSkipException('vacuum=False — пропущено')

        timeout = 15 * 60
        tables = context['ti'].xcom_pull(task_ids='clean') or []
        if not tables:
            raise AirflowSkipException('нет таблиц из clean')
        # Штатный пользователь Airflow не владеет таблицами main и VACUUM их молча
        # пропускает — идём админским коннектом из Vault, проверив его права заранее
        problem = af_owner_problem(tables)
        if problem:
            add_note(problem, context=context, level='DAG,Task', title='🧹 vacuum ☮️')
            raise AirflowSkipException(problem)
        before = db_stats(tables)
        conn_id = get_af_conn()

        results, skipped = [], []
        n = len(tables)
        # Как у clean: строка на таблицу — ход виден, пока идёт вакуум (до 15 мин на таблицу);
        # в конце строки с мёртвыми до/после и итог над ними
        feed = Feed(context, '🧹 vacuum')
        for i, tbl in enumerate(tables, 1):
            _ts = time.time()
            icon, why = '✅', ''
            try:
                db_vacuum(tbl, conn_id, full=False, timeout=timeout)
            except AirflowSkipException as e:
                logger.warning(f"☮️ {tbl}: {e}")
                icon, why = '☮️', str(e)[:60]
            except Exception as e:
                logger.warning(f"⚠️ {tbl}: {e}")
                icon, why = '❌', str(e)[:60]
            row = {'table': tbl, 'duration': round(time.time() - _ts, 2), 'status': f'{icon} {why}'.strip()}
            (results if icon == '✅' else skipped).append(row)
            feed.line(f"{icon} {tbl} ({i}/{n}): {row['duration']} с" + (f" — {why}" if why else ''))

        if not results and not skipped:
            add_note('нет таблиц для вакуума', context=context, level='DAG,Task', title='🧹 vacuum')
            return

        # Даём коллектору статистики дописать результаты вакуума (PGSTAT_STAT_INTERVAL = 500 мс)
        if results:
            time.sleep(2)
        after = db_stats(tables) if results else {}
        for r in results:
            r['ok'] = True
        for r in results + skipped:
            dead_b = before.get(r['table'], (None, None))[0]
            dead_a, last_vac = after.get(r['table'], (None, None))
            r['dead'] = f"{dead_b} → {dead_a}" if r.get('ok') else str(dead_b)
            r['last_vacuum'] = _fmt_ts(last_vac)
            logger.info(f"🔎 {r['table']}: мёртвых {r['dead']} | last_vacuum={last_vac}")

        total = round(sum(r['duration'] for r in results + skipped), 2)
        summary = (f'{len(results)}/{len(tables)} таблиц за {total} с'
                   + (f' | ☮️ пропущено {len(skipped)}' if skipped else ''))
        feed.done(summary, keep=[f"{r['status'][:2].strip()} {r['table']}: {r['duration']} с, мёртвых {r['dead']}"
                                 + (f" — {r['status'][2:].strip()}" if r['status'][2:].strip() else '')
                                 for r in skipped + results])
        add_note(summary, context=context, level='DAG', title='🧹 vacuum')

    @task(task_id='report', trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        """Сводка рана: сколько удалено по таблицам, сравнение с прошлым запуском."""
        from airflow.models import DagRun, XCom
        from airflow.utils.session import create_session

        dag_id = context['dag_run'].dag_id
        run_id = context['dag_run'].run_id

        with create_session() as session:
            prev_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id, DagRun.run_id != run_id)
                .order_by(DagRun.execution_date.desc())
                .first()
            )
        prev_data = None
        if prev_run:
            prev_data = XCom.get_one(
                run_id=prev_run.run_id, key='return_value',
                task_id='report', dag_id=dag_id,
            )
        before = {r['table']: r['size_bytes'] for r in prev_data} if prev_data else {}

        sql = """
            SELECT
                relname,
                pg_total_relation_size('main.' || relname)  AS total_bytes,
                n_live_tup,
                n_dead_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'main'
            ORDER BY total_bytes DESC
        """
        with create_session() as session:
            rows = session.execute(text(sql)).fetchall()

        data = []
        for relname, total_bytes, live, dead in rows:
            after_b  = total_bytes or 0
            before_b = before.get(relname)
            delta_b  = (after_b - before_b) if before_b is not None else None
            delta_s  = (('-' if delta_b < 0 else '+') + readable_size(abs(delta_b))) if delta_b else ''
            data.append({
                'table':      relname,
                'after':      readable_size(after_b),
                'delta':      delta_s,
                'size_bytes': after_b,
                'live_rows':  readable_size(live or 0, base=1000),
                'dead_rows':  readable_size(dead or 0, base=1000),
            })

        lines = [f"{r['table']}: {r['after']}" + (f" ({r['delta']})" if r['delta'] else '')
                 + f", записей {r['live_rows']}, мёртвых {r['dead_rows']}" for r in data]
        logger.info("📊 Отчёт по схеме main:\n" + '\n'.join(lines))

        total_after = sum(r[1] or 0 for r in rows)
        total_live  = sum(r[2] or 0 for r in rows)
        total_dead  = sum(r[3] or 0 for r in rows)
        if before:
            total_before = sum(before.get(r[0], r[1] or 0) for r in rows)
            total_delta  = total_after - total_before
            delta_str    = (('-' if total_delta < 0 else '+') + readable_size(abs(total_delta))) if total_delta else '-'
            before_str   = readable_size(total_before)
        else:
            delta_str  = '—'
            before_str = '—'
        summary = (f"{len(rows)} таблиц: {before_str} → {readable_size(total_after)} ({delta_str}), "
                   f"записей {readable_size(total_live, base=1000)}, мёртвых {readable_size(total_dead, base=1000)}")
        # Таблицы — от крупных: в 1000 символов заметки влезают главные
        Feed(context, '📊 Схема main').done(summary, keep=lines)
        if not ADMIN:
            summary += ('\n\n☮️ Админской учётки метабазы в Vault нет: вакуум, переиндексация и '
                        'удаление остатков не создаются — статистику ведёт автовакуум')
        add_note(summary, context=context, level='DAG', title='📊 Схема main')

        return data

    @task(task_id='integrity', trigger_rule=TriggerRule.ALL_DONE)
    def integrity(**context):
        """🩺 Целостность метабазы: индексы, wraparound, последовательности, вакуум, транзакции."""
        # Как у clean и vacuum: строка на проверку по мере выполнения, в конце итог над ними —
        # сначала находки, потом здоровые
        feed = Feed(context, '🩺 integrity')
        checks = {}
        for fn in INTEGRITY_CHECKS:
            name = fn.__name__.removeprefix('check_')
            c = checks[name] = _run_check(fn)
            feed.line(f"{CHECK_ICON[c['status']]} {name} ({c['sec']} с) — {c['summary']}")
        hint = ''
        if checks['indexes'].get('leftovers'):
            hint = ('Удалить остатки: ручной запуск с галкой `drop_leftovers` — отдельный таск в начале '
                         'рана, исходные индексы проверяются перед удалением' if ADMIN else
                         'Удалить остатки — вручную одной командой `DROP INDEX` с `lock_timeout`: админской учётки '
                         'метабазы в Vault нет, галки `drop_leftovers` в форме нет')
        bad = [n for n, c in checks.items() if c['status'] != 'healthy']
        feed.done(f"{len(checks) - len(bad)}/{len(checks)} в порядке" + (f", находки: {', '.join(bad)}" if bad else ''),
                  keep=sorted(feed.lines, key=lambda ln: ln.startswith('✅')), rest=hint)
        push_health({n: {k: v for k, v in c.items() if k in ('status', 'summary', 'sec')}
                     for n, c in checks.items()}, context)
        return {n: c['status'] for n, c in checks.items()}

    # Первым после params: с остатками каждая запись в таблицу и VACUUM обновляют все лишние
    # индексы (сигма dev 01.10.2026 — 551 на dag_run, шаг шедулера 35 с), а перестройка рядом с
    # чужим *_ccnew назвала бы свою копию *_ccnew1. dry_run галку не отменяет: она явная,
    # а dry_run про строки clean (так же у reindex с v2.9)
    @task(task_id='drop_leftovers', trigger_rule=TriggerRule.NONE_FAILED)
    def drop_leftovers_task(**context):
        """🩹 Остатки прерванного REINDEX — только по разовой галке, до тяжёлых тасков."""
        from airflow.exceptions import AirflowSkipException

        if not context['params'].get('drop_leftovers'):
            raise AirflowSkipException('галка drop_leftovers не стоит')
        leftovers = _run_check(check_indexes).get('leftovers') or []
        if not leftovers:
            add_note('🩹 остатков прерванного REINDEX нет', context=context, level='Task')
            return {'done': 0, 'kept': 0}
        started = time.time()
        done, kept = drop_leftovers(leftovers)
        add_note(f'🧹 остатков удалено {len(done)}, оставлено {len(kept)}, {time.time() - started:.0f} с'
                 + ''.join(f'\n- {k}' for k in kept[:20]), context=context, level='Task', title='🩹 drop_leftovers')
        return {'done': len(done), 'kept': len(kept)}

    @task(task_id='reindex', trigger_rule=TriggerRule.ALL_DONE)
    def reindex(**context):
        """🔁 Переиндексация по одному индексу — только по разовой галке; dry_run её не отменяет."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        p = context['params']
        if not p.get('reindex'):
            raise AirflowSkipException('галка reindex не стоит')
        feed = Feed(context, '🔁 reindex')

        def step(r, error):
            # Как у clean и vacuum: строка на индекс по ходу, в конце итог над главными строками
            feed.line(f"{'❌' if error else '✅'} {r['idx']} ({r['tbl']}): "
                      + (error[:100] if error else f"{readable_size(r['bytes'])} → "
                         f"{readable_size(r['after']) if r['after'] is not None else '—'}, {r['sec']} с"))

        res = reindex_one_by_one(on_step=step)
        if isinstance(res, str):
            add_note(res, context=context, level='DAG,Task', title='🔁 reindex ☮️')
            raise AirflowSkipException(res)
        done, failed, left = res
        # В итоге — отказы и индексы, похудевшие от REINDEX_NOTE_MIN, крупные первыми;
        # мелочь (8 КБ → 8 КБ) одной строкой «ещё N»
        lines = [f"❌ {r['idx']} ({r['tbl']}): {r['error'][:100]}"
                 + (f"; копия удалена: {', '.join(r['dropped'])}" if r['dropped'] else '')
                 + (f"; копия осталась: {', '.join(r['kept'])}" if r['kept'] else '') for r in failed]
        gained = sorted((r for r in done if r['bytes'] - (r['after'] or r['bytes']) >= REINDEX_NOTE_MIN),
                        key=lambda r: r['bytes'] - (r['after'] or r['bytes']), reverse=True)
        lines += [f"✅ {r['idx']} ({r['tbl']}): {readable_size(r['bytes'])} → {readable_size(r['after'])}, "
                  f"{r['sec']} с" for r in gained]
        rest = (f"остальные {len(done) - len(gained)} — меньше {readable_size(REINDEX_NOTE_MIN)} разницы"
                if len(done) > len(gained) else '')
        if left:
            rest += (f"\n⏱️ не начаты (бюджет {REINDEX_BUDGET_SEC // 3600} ч или отказ на таблице): {len(left)}, "
                     "крупнейшие — " + ', '.join(f"{r['idx']} ({readable_size(r['bytes'])})" for r in left[-3:]))
        saved = sum(r['bytes'] - (r['after'] or r['bytes']) for r in done)
        summary = (f"перестроено {len(done)}, освобождено {readable_size(max(saved, 0))}"
                   + (f", отказов {len(failed)}" if failed else '') + (f", не начаты {len(left)}" if left else ''))
        feed.done(summary, keep=lines, rest=rest.strip())
        add_note(summary, context=context, level='DAG', title='🔁 reindex')
        if failed:
            raise AirflowFailException(f"отказов {len(failed)}: " + ', '.join(r['idx'] for r in failed))
        return summary

    params_done = save_params()
    tail = clean()
    if ADMIN:
        params_done >> drop_leftovers_task() >> tail
    else:
        params_done >> tail
    if ADMIN:
        # integrity — после переиндексации: иначе приняла бы копию идущей перестройки за
        # остаток, а копию от убитой по таймауту — поймает
        vacuumed, reindexed = vacuum(), reindex()
        tail >> vacuumed >> reindexed
        tail = reindexed
    tail >> report()
    tail >> integrity() >> health_tasks(ttl_sec=REPORT_TTL_SEC, skill='tools-db-cleanup')

tools_db_cleanup()
