"""🧪 DAG: проверка оператора HrpS3ToPgStreamOperator (S3 → GP/PG) по сценариям спеки.
*2026-10-15 13:00 MSK · v0.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Кладёт в S3 маленькие файлы-фикстуры, создаёт таблицы `<schema>.pg_stream_t_*`, гоняет
оператор (`op.execute(context)`) по сценариям: усечение none/hard/soft/auto, откат при
битой строке, опции COPY, счётчики строк, таймаут; на Greenplum — отбраковка
`LOG ERRORS SEGMENT REJECT LIMIT`. В конце убирает таблицы и файлы.

Таски: `scenarios` — все сценарии подряд, первый провал роняет таск.

Подробно: [tools/readme.md — test_pg_stream](../../_plugin_dag_docs/?doc=tools/readme.md#test_pg_streampytest_pg_streampy)
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models.param import Param

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
    from plugins.pg_stream import HrpS3ToPgStreamOperator  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
    from CI06932748.tools.pg_stream import HrpS3ToPgStreamOperator  # type: ignore

logger = logging.getLogger("airflow.task")

ensure_pool(TOOLS_POOL)

CSV3 = b"id,name\n1,a\n2,b\n3,c\n"


class _Grab(logging.Handler):
    """Сообщения лога оператора: сценарии проверяют, что он написал."""

    def __init__(self):
        super().__init__(logging.INFO)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())

    def has(self, text):
        return any(text in s for s in self.lines)


_DEF_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             0,
    "pool":                TOOLS_POOL,
    "execution_timeout":   timedelta(minutes=30),
    "on_failure_callback": on_callback,
    "on_success_callback": on_callback,
}


@dag(
    dag_id="tools_test_pg_stream",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=40),
    tags=["DataTools", "tools", "AutoQA"],
    default_args=_DEF_ARGS,
    owner_links={
        "DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab",
        "Nick Churkin": "mailto:NSChurkin@sber.ru",
    },
    doc_md=__doc__,
    description="Сценарии HrpS3ToPgStreamOperator: S3 → GP/PG, усечение, откат, счётчики строк",
    params={
        "conn_id": Param("cap_gp_etl", type="string", title="PG/GP conn_id",
                         examples=["cap_gp_etl", "gp6-stand"],
                         description="Подключение Postgres-типа: GP или PostgreSQL; нужны права создавать таблицы в schema"),
        "bucket":  Param("edpetl-ctl", type="string", title="S3 бакет",
                         description="Бакет для файлов-фикстур (подключение s3)"),
        "prefix":  Param("test/pg_stream/", type="string", title="Префикс S3",
                         description="Папка фикстур; после рана удаляется"),
        "schema":  Param("public", type="string", title="Схема",
                         description="Схема для таблиц pg_stream_t_*; после рана они удаляются"),
    },
)
def tools_test_pg_stream():

    @task
    def scenarios(**context):
        """Оператор по сценариям спеки на заданном подключении, затем уборка."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        p = context["params"]
        conn_id, bucket, prefix, schema = p["conn_id"], p["bucket"], p["prefix"], p["schema"]
        s3, pg = S3Hook(aws_conn_id="s3"), PostgresHook(postgres_conn_id=conn_id)
        keys, tables, done = [], [], []

        def put(name, data):
            key = prefix + name
            s3.load_bytes(data, key, bucket, replace=True)
            keys.append(key)
            return key

        def run(name, key, table, **kw):
            op = HrpS3ToPgStreamOperator(task_id=f"pg_stream_{len(done)}", s3_key=key, table=table,
                                         bucket=bucket, conn_id=conn_id, **kw)
            grab = _Grab()
            op.log.addHandler(grab)
            try:
                res = op.execute(context)
            finally:
                op.log.removeHandler(grab)
            logger.info("%s: %s", name, res)
            return res, grab

        def ok(name):
            done.append(name)
            logger.info("✅ %s", name)

        with closing(pg.get_conn()) as db:
            db.autocommit = True
            cur = db.cursor()

            def q(text, args=None):
                cur.execute(text, args)
                return cur.fetchall() if cur.description else None

            gp = "greenplum" in q("select version()")[0][0].lower()
            dist = " distributed randomly" if gp else ""

            def mk(name, cols):
                t = f"{schema}.pg_stream_t_{name}"
                q(f"drop table if exists {t}")
                q(f"create table {t} ({cols}){dist}")
                tables.append(t)
                return t

            def count(t):
                return q(f"select count(*) from {t}")[0][0]

            try:
                t = mk("basic", "id int, name text")
                k3 = put("basic.csv", CSV3)

                # REQ-12, 11, 10: без усечения дважды — строк вдвое; счётчики, типы, таймаут
                res, log = run("none", k3, t)
                run("none-2", k3, t)
                assert count(t) == 6, count(t)
                assert (res["rows_source"], res["rows_loaded"], res["rows_rejected"]) == (3, 3, 0), res
                assert res["target"] == {"id": "int4", "name": "text"}, res["target"]
                assert res["source"] == {"id": "text", "name": "text"}, res["source"]
                assert res["files"] == [k3], res["files"]
                assert log.has("statement_timeout 240min") and log.has("усечение: нет"), log.lines
                ok("REQ-12/11/10 без усечения, счётчики, типы, таймаут по умолчанию")

                # REQ-13, 10: hard и заданный таймаут
                res, log = run("hard", k3, t, truncate="hard", statement_timeout=5)
                assert count(t) == 3 and log.has("statement_timeout 5min") and log.has("усечение: truncate"), log.lines
                ok("REQ-13 hard, REQ-10 таймаут 5 мин")

                # REQ-14: soft — читатель во время загрузки не ждёт и видит старые 3 строки
                big = put("big.csv", b"id,name\n" + b"".join(b"%d,n%d\n" % (i, i) for i in range(500_000)))
                reads, errors, stop = [], [], threading.Event()

                def reader():
                    with closing(pg.get_conn()) as rc:
                        rc.autocommit = True
                        rcur = rc.cursor()
                        rcur.execute("set statement_timeout = '2s'")
                        while not stop.is_set():
                            try:
                                # чужая блокировка сильнее чтения — значит, загрузка идёт. Два
                                # запроса: GP не строит план, где pg_locks (мастер) рядом с таблицей сегментов
                                rcur.execute(f"""select exists(select 1 from pg_locks where relation = '{t}'::regclass
                                           and pid <> pg_backend_pid() and mode <> 'AccessShareLock')""")
                                locked = rcur.fetchone()[0]
                                rcur.execute(f"select count(*) from {t}")
                                reads.append((rcur.fetchone()[0], locked))
                            except Exception as e:     # таймаут чтения = провал сценария
                                errors.append(repr(e))
                            time.sleep(0.02)

                th = threading.Thread(target=reader, daemon=True)
                th.start()
                time.sleep(0.5)
                try:
                    res, log = run("soft", big, t, truncate="soft")
                finally:
                    stop.set()
                    th.join(10)
                during = [n for n, locked in reads if locked]
                logger.info("soft: чтений %s, во время загрузки %s, ошибок %s", len(reads), len(during), errors)
                assert not errors, errors[:3]
                assert during and set(during) == {3}, reads
                assert count(t) == 500_000 and log.has("усечение: delete") and res["rows_loaded"] == 500_000
                ok("REQ-14 soft: чтение не ждёт и видит старые строки")

                # REQ-15: auto — свободная таблица → truncate; занятая чужим чтением → delete без ожидания
                res, log = run("auto", k3, t, truncate="auto")
                assert count(t) == 3 and log.has("усечение: truncate"), log.lines
                with closing(pg.get_conn()) as other:
                    other.cursor().execute(f"lock table {t} in access share mode")
                    started = time.monotonic()
                    # таймаут 1 мин: если оператор ждёт блокировку, сценарий падает, а не висит
                    res, log = run("auto-busy", k3, t, truncate="auto", statement_timeout=1)
                    took = time.monotonic() - started
                    other.rollback()
                assert count(t) == 3 and log.has("усечение: delete") and took < 30, (took, log.lines)
                ok("REQ-15 auto: truncate / delete при занятой таблице")

                # REQ-08: битая строка на 1000-й — откат всего, включая усечение
                q(f"truncate {t}")
                q(f"insert into {t} select g, 'old' from generate_series(1, 5) g")
                bad = put("bad.csv", b"id,name\n" + b"".join(b"%d,x\n" % i for i in range(998)) + b"oops,x\n1,y\n")
                try:
                    run("rollback", bad, t, truncate="hard")
                except Exception as e:
                    logger.info("ожидаемая ошибка: %r", e)
                else:
                    raise AssertionError("битая строка загрузилась")
                assert count(t) == 5, count(t)
                ok("REQ-08 откат при ошибке, усечение тоже откатилось")

                # REQ-19: опции COPY
                semi = put("semi.csv", b"id;name\n1;a\n2;b\n3;c\n")
                res, _ = run("semicolon", semi, t, truncate="hard", options="CSV HEADER DELIMITER ';'")
                assert count(t) == 3 and res["rows_source"] == 3, res
                ok("REQ-19 options с разделителем ';'")

                # REQ-11: перевод строки внутри кавычек — одна запись
                ml = put("multiline.csv", b'id,name\n1,"a\nb"\n2,"c,\n\nd"\n')
                res, _ = run("multiline", ml, t, truncate="hard")
                assert (res["rows_source"], res["rows_loaded"], res["rows_rejected"]) == (2, 2, 0), res
                ok("REQ-11 rows_source считает запись с переводом строки одной")

                # REQ-16, 17: ошибки параметров — до S3 и базы
                try:
                    HrpS3ToPgStreamOperator(task_id="bad_trunc", s3_key=k3, table=t, truncate="yes")
                except ValueError:
                    pass
                else:
                    raise AssertionError("truncate='yes' принят")
                try:
                    run("txt", prefix + "nothing.txt", t)
                except ValueError as e:
                    assert "nothing.txt" in str(e), e
                else:
                    raise AssertionError(".txt без format принят")
                ok("REQ-16/17 ошибки параметров до S3 и базы")

                # REQ-11 на GP: отбраковка LOG ERRORS — rows_rejected и предупреждение
                if gp:
                    tr = mk("rej", "id int, name text, d date")
                    rej = put("rej.csv", b"id,name,d\n1,a,2026-01-01\n2,b,2026-01-02\n3,c,31.31.2026\n"
                                         b"4,d,2026-01-04\n5,e,2026-01-05\n")
                    res, log = run("reject", rej, tr, options="CSV HEADER LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS")
                    assert (res["rows_source"], res["rows_loaded"], res["rows_rejected"]) == (5, 4, 1), res
                    assert count(tr) == 4 and log.has("не загружено строк: 1"), log.lines
                    assert res["target"]["d"] == "date", res["target"]
                    ok("REQ-11 GP: отбраковка 5 → 4 + 1")
                else:
                    logger.info("☮️ отбраковка LOG ERRORS — только Greenplum, пропуск")
            finally:
                for t in tables:
                    q(f"drop table if exists {t}")
                if keys:
                    s3.delete_objects(bucket, keys)

        add_note(f"✅ {conn_id} ({'GP' if gp else 'PG'}): сценариев {len(done)}", context, level="DAG", add=True)
        return done

    scenarios()


tools_test_pg_stream()
