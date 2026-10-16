"""🧪 DAG: загрузка JSON-массива в PostgreSQL или Greenplum через COPY — ручная проверка.
*2026-10-15 13:24 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Проверяет приём JSON в PG/GP целиком (тип базы — по `version()`, код один): массив
читается потоково — синтетический или файл из S3, в том числе в конверте Kafka Connect
`{"data": [...]}` (поток ТФС → GP), — каждый элемент уходит строкой в `jsonb` пачками COPY,
затем раскладывается по полям и сверяется. Пишет только во временную таблицу — после рана
в базе ничего не остаётся.

Таски: `load_check` — загрузка, раскладка и сверка в одной транзакции.

Подробно: [tools/readme.md — test_json_pg](../../_plugin_dag_docs/?doc=tools/readme.md#test_json_pgpytest_json_pgpy)
"""
from __future__ import annotations

import codecs
import io
import json
import logging
from contextlib import closing
from datetime import datetime, timedelta, timezone
from itertools import islice

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
    from plugins.pg_stream import iter_json_array  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
    from CI06932748.tools.pg_stream import iter_json_array  # type: ignore

logger = logging.getLogger("airflow.task")

ensure_pool(TOOLS_POOL)

PG_CONN = "cap_gp_etl"
S3_CONN = "s3"
CHUNK = 1 << 20     # байт за одно чтение из S3

# Текстовый формат COPY съедает обратные слэши (\" и \n внутри JSON ломаются), поэтому csv
# с кавычкой и разделителем, которых в JSON не бывает: строка файла уходит в колонку как есть.
COPY_SQL = "copy raw_json(j) from stdin with (format csv, quote e'\\x01', delimiter e'\\x02')"

# Строка с тем, что ломает наивную загрузку: кавычки, обратный слэш, `,]` разбора массива,
# перевод строки и табуляция (вне csv-кавычек перевод строки закончил бы запись), кириллица.
NASTY = 'Имя "q" \\ ,] \n\t'

def _synthetic(rows: int):
    """Синтетический массив кусками: элементы с отступами (многострочные) и NASTY в имени."""
    yield "[\n"
    for i in range(rows):
        item = {"id": i, "name": f"{NASTY}{i}", "tags": [i, None, True]}
        yield ("," if i else "") + json.dumps(item, ensure_ascii=False, indent=1)
    yield "\n]"


def _s3_chunks(bucket: str, key: str):
    """Текст объекта S3 кусками; инкрементальный декодер не рвёт кириллицу на границе куска."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    obj = S3Hook(aws_conn_id=S3_CONN).get_key(key, bucket)
    if obj is None:
        raise AirflowFailException(f"нет объекта s3://{bucket}/{key}")
    utf8 = codecs.getincrementaldecoder("utf-8")()
    with closing(obj.get()["Body"]) as body:   # и при обрыве разбора: соединение S3 не висит
        for chunk in body.iter_chunks(CHUNK):
            yield utf8.decode(chunk)
    yield utf8.decode(b"", final=True)


_DEF_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             0,
    "pool":                TOOLS_POOL,
    "execution_timeout":   timedelta(minutes=30),
    "on_failure_callback": on_callback,
    "on_success_callback": on_callback,
}


@dag(
    dag_id="tools_test_json_pg",
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
    description="PG/GP: потоковая загрузка JSON-массива через COPY во временные таблицы и сверка",
    params={
        "conn_id": Param(PG_CONN, type="string", title="PG/GP conn_id",
                         examples=[PG_CONN, "alpha-adb_dev_comm-read", "airflowdb"],
                         description="Подключение Postgres-типа: GP или PostgreSQL. Хватает читающего (etl): пишем только во временные таблицы"),
        "rows":    Param(100_000, type="integer", minimum=1, title="Строк (синтетика)",
                         description="Сколько элементов в синтетическом массиве; при заданном s3_key не используется"),
        "batch":   Param(50_000, type="integer", minimum=1, title="Строк на COPY",
                         description="Размер пачки: столько строк держится в памяти воркера за раз"),
        "bucket":  Param("", type="string", title="S3 бакет",
                         description="Бакет файла (подключение s3); вместе с s3_key"),
        "s3_key":  Param("", type="string", title="S3 ключ",
                         description="Файл с JSON-массивом. Пусто (и bucket пуст) — синтетический массив из rows элементов"),
        "root":    Param("", type="string", title="Ключ массива",
                         examples=["", "data"],
                         description="Массив в объекте под этим ключом: data — конверт Kafka Connect {\"data\": [...]} файлов ТФС → GP. Пусто — файл и есть массив"),
    },
)
def tools_test_json_pg():

    @task
    def load_check(**context):
        """Загрузка массива в temp-таблицу пачками COPY, раскладка по полям, сверка."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        params = context["params"]
        key, bucket = params["s3_key"].strip(), params["bucket"].strip()
        if bool(key) != bool(bucket):   # иначе молча ушли бы в синтетику или упали без ясной причины
            raise AirflowFailException("bucket и s3_key задаются вместе")
        items = iter_json_array(_s3_chunks(bucket, key), params["root"].strip()) if key else iter_json_array(_synthetic(params["rows"]))

        # Всё в одной транзакции: temp-таблицы on commit drop уходят с её откатом в конце.
        # closing — потому что with по соединению psycopg2 закрывает транзакцию, а не соединение.
        with closing(PostgresHook(postgres_conn_id=params["conn_id"]).get_conn()) as conn, conn.cursor() as cur:
            cur.execute("select version()")
            # DISTRIBUTED есть только у GP, в PostgreSQL это синтаксическая ошибка.
            dist = " distributed randomly" if "greenplum" in cur.fetchone()[0].lower() else ""
            cur.execute(f"create temp table raw_json (j jsonb) on commit drop{dist}")

            total = 0
            while batch := list(islice(items, params["batch"])):
                cur.copy_expert(COPY_SQL, io.StringIO(
                    "\n".join(json.dumps(x, ensure_ascii=False) for x in batch)))
                total += len(batch)
                logger.info("Загружено %s строк", total)

            cur.execute("select count(*) from raw_json")
            raw_cnt = cur.fetchone()[0]

            # Раскладка по полям: сколько строк несёт каждое поле. У конверта Kafka Connect
            # поля в payload, у прочих — в самом элементе; схема у элементов может быть разной.
            cur.execute("""
                select e.key, count(*), count(*) filter (where e.value <> 'null'::jsonb)
                from raw_json,
                     jsonb_each(case when jsonb_typeof(j->'payload') = 'object' then j->'payload'
                                     when jsonb_typeof(j) = 'object' then j end) e
                group by e.key order by e.key""")
            fields = {k: {"rows": n, "filled": f} for k, n, f in cur.fetchall()}
            logger.info("Поля (строк / заполнено):\n%s",
                        "\n".join(f"  {k:<40} {v['rows']:>9} {v['filled']:>9}" for k, v in fields.items()))

            bad = 0
            if not key:
                # Каждое имя должно вернуться байт в байт: проверка экранирования на всём пути.
                cur.execute("select count(*) from raw_json where j->>'name' is distinct from %s || (j->>'id')",
                            (NASTY,))
                bad = cur.fetchone()[0]
            conn.rollback()     # тест: ничего не фиксируем, temp-таблица уходит с откатом

        msg = f"элементов {total}, raw_json {raw_cnt}, полей {len(fields)}" + ("" if key else f", искажённых имён {bad}")
        # Синтетика сверяется и с заданным числом: счётчик total — то, что отдал разбор,
        # и потерю элементов в нём он не заметил бы.
        if raw_cnt != total or bad or (not key and (total != params["rows"] or len(fields) != 3)):
            raise AirflowFailException(f"❌ Расхождение: {msg}")
        add_note(f"✅ {'s3://' + bucket + '/' + key if key else 'синтетика'}: {msg}",
                 context, level="DAG", add=True)
        return {"total": total, "raw": raw_cnt, "bad_names": bad, "fields": fields}

    load_check()


tools_test_json_pg()


if __name__ == "__main__":   # самопроверка разбора: python3 tools/test_json_pg.py
    data = [x for i in range(50) for x in ({"id": i, "s": NASTY}, 7, 12.5e3, [1, 2], None, True)]
    text = json.dumps(data, ensure_ascii=False, indent=2)
    for n in (1, 3, 17, 4096):
        assert list(iter_json_array(text[i:i + n] for i in range(0, len(text), n))) == data, n
    assert list(iter_json_array([" [ ] \n"])) == []
    env = json.dumps({"data": data}, ensure_ascii=False)
    for n in (1, 5, 4096):
        assert list(iter_json_array((env[i:i + n] for i in range(0, len(env), n)), "data")) == data, n
    for bad in ('{"x": []}', '{"data": [1]', '{"data": [1]} x', '{"data" [1]}', "[1]"):
        try:
            list(iter_json_array([bad], "data"))
        except ValueError:
            continue
        raise AssertionError(bad)
    assert list(iter_json_array(_synthetic(5))) == [{"id": i, "name": f"{NASTY}{i}", "tags": [i, None, True]} for i in range(5)]
    for bad in ("[1,,2]", "[,1]", "[1 2]", "[1,]", "[1] x", "[1][2]", "[1", "{}", ""):
        try:
            list(iter_json_array([bad]))
        except ValueError:
            continue
        raise AssertionError(bad)
    print("ok")
