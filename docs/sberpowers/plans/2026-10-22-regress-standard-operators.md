# План: стандартные операторы в регрессе test_hrp_operators (AF3)

> Исполняется скиллом execute. Исполнитель — я сам (feedback_sberpowers_flow), ревью — DeepSeek
> v4-pro + agy в фоне после последней задачи. Шаги — чекбоксы `- [ ]`.

**Спека:** docs/sberpowers/specs/2026-10-22-regress-standard-operators.md (REQ-hrp-regress-01, 34…46)
**Цель:** `test_hrp_operators` в AF3 гоняет 20 стандартных операторов (14 + 6 по поправке 22.10, волна 6) в главной группе `test_standart`;
`verdict` и `report` учитывают их наравне с Hrp-группами.
**Архитектура:** тот же реестр `Op` (поле `pkg` уже есть — загрузчик `_load` не меняется), второй список
`STD_OPS`, сборка дага — цикл по двум главным группам. Ветвление AF3 (`skip_all_except`/`skip` берут `ti`
задачи-теста и пропустили бы следующий тест) подменяется mock-ом на классе оператора, вызов сверяется.
`TriggerDagRunOperator` бросает `DagRunTriggerException` — тест ловит его и сам отдаёт супервизору
через `task_runner._handle_trigger_dag_run`.

## Глобальные ограничения
- Репозиторий `~/ctl` (af_dags), ветка `feat/HRPDATALAB-14480-regress-standard` от `develop_3`; AF2 (`master`) не трогать.
- Python 3.10, AF3 3.3.1 (`airflow.sdk`); провайдеры стенда/контура: standard, common-sql 2.1.0, amazon 9.36.0, airflow-clickhouse-plugin 1.8.0.
- Без deferrable, `mode="poke"`, `wait_for_completion=False`.
- Контракт DPM: `dag_id=test_hrp_operators`, задача `report`, ключи `verdict` прежние; имена групп в `verdict` без префикса главной группы.
- Окончания строк файла не менять (`git ls-files --eol tools/test_hrp_operators.py` до и после).
- Строка версии docstring: `TZ=Europe/Moscow date '+%Y-%m-%d %H:%M'`, v2.7, автор прежний.
- `ruff check tools/` — до каждого коммита.
- Стенд: AF3, `AIRFLOW_HOME=/opt/aftest/home3`, venv `/opt/aftest/venv312`, даги `home3/dags/tools/`; файл — `scp` + сверка md5; `ssh testsrv '…' </dev/null`.

## Волны
Все задачи правят один файл `tools/test_hrp_operators.py` → строго последовательно.
- Волна 1: 1.1 — каркас `test_standart` + Python-семейство (нет блокеров)
- Волна 2: 2.1 — ветвление и `@task.sensor` (blocked by 1.1)
- Волна 3: 3.1 — SQL и ClickHouse (blocked by 2.1: использует `_no_skip`, `_sensor_*`)
- Волна 4: 4.1 — S3 (blocked by 2.1)
- Волна 5: 5.1 — TriggerDagRun, документация, полный ран (blocked by 3.1, 4.1)
- Волна 6: 6.1 — ещё 6 операторов по замеру dev за всё время, полный ран (blocked by 5.1; поправка 22.10)

## Деплой и прогон на стенде (общий рецепт для «Критериев выхода»)
```bash
cd ~/ctl && scp tools/test_hrp_operators.py testsrv:/opt/aftest/home3/dags/tools/ \
 && md5sum tools/test_hrp_operators.py && ssh testsrv 'md5sum /opt/aftest/home3/dags/tools/test_hrp_operators.py' </dev/null
ssh testsrv '. /opt/aftest/af3/scripts/stand_af3_env.sh; airflow dags reserialize >/dev/null 2>&1; \
  airflow dags list-import-errors; airflow tasks list test_hrp_operators | grep test_standart' </dev/null
# прогон: только новые группы — Hrp выключены через conf (op_<hrp>=False)
ssh testsrv '. /opt/aftest/af3/scripts/stand_af3_env.sh; airflow dags trigger test_hrp_operators -c "$CONF"' </dev/null
```
`CONF` собрать скриптом: `{"op_<g>": false}` для каждой Hrp-группы. Ждать в фоне (`run_in_background`,
until-цикл по `airflow dags state test_hrp_operators <run_id> | cut -d, -f1`). Исходы — MCP `airflow-test`
(`list_tasks`/`get_task_instance`) или заметка `report`.

---

### Задача 1.1: каркас test_standart и Python-семейство (волна 1)

**REQ:** REQ-hrp-regress-34, 35, 36, 39, 44, 01
**model-hint:** any
**Blocked by:** нет блокеров

**Контекст:** заводит вторую главную группу и прогоняет через неё самые простые операторы — Empty, Python,
`@task`. На каркас опираются все следующие задачи.

**Файлы:** изменить `tools/test_hrp_operators.py` (реестр после `OPS`, `report`, params, сборка дага, docstring).

**Интерфейсы:**
- Производит: `STD = "airflow.providers"`; `STD_OPS: list[Op]`; `TOPS = ("tests.", "test_standart.")`;
  `_ctx_fn(a, b=0, **kw) -> dict`; тесты `empty_run`, `python_args`, `python_tpl`, `task_args`, `task_tpl`.

**Шаги:**
- [ ] Ветка: `git switch -c feat/HRPDATALAB-14480-regress-standard develop_3`; `git ls-files --eol tools/test_hrp_operators.py`.
- [ ] Тесты Python-семейства (раздел перед `OPS`):
```python
# ─────────────────────────── Тесты: стандартные — Python ──────────────────────────
def _ctx_fn(a, b=0, **kw):
    """Вызываемое Python-операторов: аргументы и контекст рана (ti, run_id)."""
    return {"a": a, "b": b, "ti": kw["ti"].task_id, "run_id": kw["run_id"]}


def _python(t: T, b, want_b):
    result, _ = t.run(python_callable=_ctx_fn, op_args=[1], op_kwargs={"b": b})
    want = {"a": 1, "b": want_b, "ti": t.context["ti"].task_id, "run_id": t.context["run_id"]}
    if result != want:
        raise AirflowFailException(f"вернула {result}, ждём {want}")


def empty_run(t: T):
    """EmptyOperator: выполняется без ошибки и ничего не возвращает"""
    result, _ = t.run()
    if result is not None:
        raise AirflowFailException(f"вернул {result!r}")


def python_args(t: T):
    """PythonOperator: op_args, op_kwargs и контекст рана в функции"""
    _python(t, 2, 2)


def python_tpl(t: T):
    """PythonOperator шаблоном: op_kwargs из {{ }} — строка '42'"""
    _python(t, "{{ 40 + 2 }}", "42")


def task_args(t: T):
    """@task: op_args, op_kwargs и контекст рана в функции"""
    _python(t, 2, 2)


def task_tpl(t: T):
    """@task шаблоном: op_kwargs из {{ }} — строка '42'"""
    _python(t, "{{ 40 + 2 }}", "42")
```
- [ ] Реестр после `OPS`/`NOT_CREATED`/`TESTS`:
```python
STD = "airflow.providers"  # стандартные операторы: классы из пакетов, которые импортируют даги контура
STD_OPS = [
    Op("empty", "standard.operators.empty", "EmptyOperator", set(), [empty_run], pkg=STD),
    Op("python", "standard.operators.python", "PythonOperator", set(), [python_args, python_tpl], pkg=STD),
    Op("task", "standard.decorators.python", "_PythonDecoratedOperator", set(), [task_args, task_tpl], pkg=STD),
]
TOPS = ("tests.", "test_standart.")  # главные группы: Hrp-операторы и стандартные
# в verdict группа — без главной группы: имена обязаны не совпадать
assert not {o.group for o in OPS} & {o.group for o in STD_OPS}, "имя группы в tests и test_standart"
```
  `TESTS` удалить, его места — на `TOPS`.
- [ ] params: после `op_` Hrp-групп
```python
        **{f"op_{op.group}": Param(SAVED.get(f"op_{op.group}", True), type="boolean",
                                   section="Стандартные операторы", description=op.cls) for op in STD_OPS},
```
  `description` дага: `"Регресс операторов hrp_operators и стандартных по группам: pg, ch, s3"`.
- [ ] Сборка — цикл по главным группам (вместо `with TaskGroup("tests", …)`):
```python
    for top, ops, tip in (("tests", OPS, "Тесты операторов: группа на оператор"),
                          ("test_standart", STD_OPS, "Стандартные операторы Airflow и провайдеров")):
        with TaskGroup(top, tooltip=tip):
            for op in ops:
                ...  # тело прежнего цикла без изменений
```
- [ ] `report`: идентификаторы тестов и блок стандартных
```python
        test_ids = sorted(tid for tid in dag_.task_ids if tid.startswith(TOPS))  # без systems и report
        full = {}  # <группа>.<тест> → task_id: в verdict без главной группы
        for task_id in test_ids:
            ...
            _, g, case = task_id.split(".", 2)
            full[f"{g}.{case}"] = task_id
            ...
                failed.append(f"{g}.{case}")
        ...
        lines = [f"❌ {tid} — {(dag_.get_task(full[tid]).doc_md or '').split(':')[0]}" for tid in failed]
        std = {o.group for o in STD_OPS}
        lines += [f"{g}: " + " ".join(f"{i}{c}" for c, i in cases.items())
                  for g, cases in sorted(groups.items()) if g not in std]
        if std & groups.keys():
            lines.append("Стандартные операторы:")
            lines += [f"{g}: " + " ".join(f"{i}{c}" for c, i in cases.items())
                      for g, cases in sorted(groups.items()) if g in std]
```
  Заголовок: `🧪 Регресс операторов {…}` вместо `HRP operators`.
- [ ] docstring: строка версии v2.7 (MSK), в абзац добавить: «Стандартные операторы Airflow и провайдеров
  из замера сигмы — в главной группе `test_standart`, по тем же правилам».
- [ ] `ruff check tools/` → `All checks passed!`; `git ls-files --eol tools/test_hrp_operators.py` — как до правки.
- [ ] Стенд по рецепту: import errors пусты; `airflow tasks list` показывает `test_standart.empty.run`,
  `test_standart.python.args|tpl`, `test_standart.task.args|tpl`; прогон с выключенными Hrp-группами — 5 ✅,
  `report` success, в заметке блок «Стандартные операторы:»; XCom `verdict.groups` содержит `empty`, `python`, `task`.
- [ ] Commit: `feat(test_hrp_operators): главная группа test_standart, Python-семейство (REQ-hrp-regress-34, 39, 44)`

**Критерии выхода:**
- ruff зелёный, EOL прежний; на стенде ран success, 5 ✅ в `test_standart`, Hrp-группы ☮️ «выключен».
- `verdict` содержит прежние ключи + `groups.empty/python/task`.

---

### Задача 2.1: ветвление и @task.sensor (волна 2)

**REQ:** REQ-hrp-regress-37, 38, 36, 45
**model-hint:** any
**Blocked by:** задача 1.1

**Контекст:** в AF3 `BranchMixIn.do_branch` и `BranchSQLOperator` вызывают `self.skip_all_except(context["ti"], …)`,
а `ShortCircuitOperator` — `self.skip(ti=…)`; `ti` здесь — задача-тест, её downstream — следующий тест.
Поэтому оба метода подменяются на классе, вызов сверяется. Сенсор в `mode="poke"` считает таймаут от
`time.monotonic()` начала `execute`, падает `AirflowSensorTimeout`.

**Интерфейсы:**
- Потребляет: `T.run`, `STD_OPS`, `STD` — задача 1.1.
- Производит: `_no_skip(t) -> contextmanager[(mock skip_all_except, mock skip)]`; `_ids(x) -> list[str]`;
  `SENSOR_TIMEOUT = 10`; `_sensor_ok(t, **kw)`, `_sensor_timeout(t, **kw)` — на них опираются 3.1 и 4.1.

**Шаги:**
- [ ] Импорт: `import time`; `from airflow.sdk.exceptions import AirflowSensorTimeout` (рядом с прочими исключениями).
- [ ] Хелперы:
```python
SENSOR_TIMEOUT = 10  # с: тест «нет» ждёт его целиком


@contextmanager
def _no_skip(t: T):
    """Ветвление AF3 пропускает downstream задачи из контекста — это задача-тест и следующий тест группы.
    Подменяем пропуск на классе оператора и сверяем, что он запросил."""
    with mock.patch.object(t.cls, "skip_all_except") as m_all, mock.patch.object(t.cls, "skip") as m_skip:
        yield m_all, m_skip


def _ids(x) -> list[str]:
    return [x] if isinstance(x, str) else sorted(x or [])


def _branch(t: T, flag, want: str, **kw):
    """Ветка выбрана по flag: execute вернул её, пропуск запрошен «всё, кроме неё»."""
    with _no_skip(t) as (m_all, _):
        result, _ = t.run(**kw)
    asked = _ids(m_all.call_args.args[1]) if m_all.called else None
    if _ids(result) != [want] or asked != [want]:
        raise AirflowFailException(f"flag={flag!r}: вернул {result!r}, пропуск кроме {asked}, ждём {want}")


def _pick(flag):
    """Вызываемое ветвления: ветка — существующая задача дага (иначе skip_all_except отвергнет id)."""
    return "systems" if flag in (True, "True") else "report"


def _sensor_ok(t: T, **kw):
    """Условие выполнено — сенсор выходит за первый poke."""
    _, m = t.run(mode="poke", poke_interval=1, timeout=SENSOR_TIMEOUT, spy="poke", **kw)
    if m.call_count != 1:
        raise AirflowFailException(f"poke вызван {m.call_count} раз, ждём 1")


def _sensor_timeout(t: T, **kw):
    """Условие не выполнено — сенсор падает по своему timeout, не раньше и не сильно позже."""
    started = time.monotonic()
    try:
        t.run(mode="poke", poke_interval=1, timeout=SENSOR_TIMEOUT, **kw)
    except AirflowSensorTimeout:
        took = time.monotonic() - started
        if not SENSOR_TIMEOUT <= took <= SENSOR_TIMEOUT + 20:
            raise AirflowFailException(f"таймаут через {took:.1f} с, ждём ~{SENSOR_TIMEOUT}") from None
        return
    raise AirflowFailException("сенсор вышел без AirflowSensorTimeout")
```
- [ ] Тесты:
```python
def branch_python_both(t: T):
    """BranchPythonOperator: обе ветки по условию, задачи регресса не пропускаются"""
    for flag, want in ((True, "systems"), (False, "report")):
        _branch(t, flag, want, python_callable=_pick, op_kwargs={"flag": flag})


def branch_python_tpl(t: T):
    """BranchPythonOperator шаблоном: условие из {{ }} — строка 'True'"""
    _branch(t, "{{ 1 == 1 }}", "systems", python_callable=_pick, op_kwargs={"flag": "{{ 1 == 1 }}"})


def _short(t: T, flag, want):
    with _no_skip(t) as (m_all, m_skip):
        result, _ = t.run(python_callable=lambda flag: flag, op_kwargs={"flag": flag})
    # у оператора в тесте нет downstream: «нет» не должно ничего пропускать
    if result != want or m_all.called or m_skip.called:
        raise AirflowFailException(f"flag={flag!r}: вернул {result!r}, skip={m_skip.called or m_all.called}")


def short_circuit_both(t: T):
    """ShortCircuitOperator: «да» и «нет», задачи регресса не пропускаются"""
    _short(t, True, True)
    _short(t, False, False)


def short_circuit_tpl(t: T):
    """ShortCircuitOperator шаблоном: условие из {{ }} — строка 'True'"""
    _short(t, "{{ 1 == 1 }}", "True")


def task_sensor_ok(t: T):
    """@task.sensor: условие выполнено — выход за первый poke"""
    _sensor_ok(t, python_callable=lambda: True)


def task_sensor_timeout(t: T):
    """@task.sensor: условие не выполнено — падение по timeout"""
    _sensor_timeout(t, python_callable=lambda: False)


def task_sensor_tpl(t: T):
    """@task.sensor шаблоном: op_kwargs из {{ }} доходит до функции"""
    _sensor_ok(t, python_callable=lambda x: x == "ok", op_kwargs={"x": "{{ 'o' ~ 'k' }}"})
```
- [ ] В `STD_OPS`:
```python
    Op("branch_python", "standard.operators.python", "BranchPythonOperator", set(),
       [branch_python_both, branch_python_tpl], pkg=STD),
    Op("short_circuit", "standard.operators.python", "ShortCircuitOperator", set(),
       [short_circuit_both, short_circuit_tpl], pkg=STD),
    Op("task_sensor", "standard.decorators.sensor", "DecoratedSensorOperator", set(),
       [task_sensor_ok, task_sensor_timeout, task_sensor_tpl], pkg=STD),
```
- [ ] Подгруппа `skip_graph` (REQ-hrp-regress-45) — настоящий пропуск. Хелперы рядом с `_pick`:
```python
SKIP_GRAPH = "test_standart.skip_graph."
# служебные задачи подгруппы: в verdict и сводке не считаются, тест — только check
GRAPH_TASKS = {SKIP_GRAPH + x for x in ("branch", "keep", "drop", "short", "after")}
GRAPH_WANT = {"branch": "success", "keep": "success", "drop": "skipped", "short": "success", "after": "skipped"}


def _graph_branch():
    return SKIP_GRAPH + "keep"
```
  В теле дага — задача-проверка:
```python
    @task(task_id="check", trigger_rule=TriggerRule.ALL_DONE, retries=0,
          doc_md="Пропуск веток в графе: Branch и ShortCircuit настоящими задачами, невыбранные ☮️")
    def skip_graph_check(**context):
        if not context["params"].get("op_skip_graph", True):
            raise AirflowSkipException("skip_graph: выключен (op_skip_graph=False)")
        ti, dr = context["ti"], context["dag_run"]
        states = ti.get_task_states(dag_id=dr.dag_id, run_ids=[dr.run_id]).get(dr.run_id, {})
        got = {x: states.get(SKIP_GRAPH + x) for x in GRAPH_WANT}
        if got != GRAPH_WANT:
            raise AirflowFailException(f"состояния {got}, ждём {GRAPH_WANT}")
```
  В цикле сборки, внутри `with TaskGroup("test_standart", …)`, после цикла по `STD_OPS`:
```python
            if all(o.loaded for o in STD_OPS if o.group in ("branch_python", "short_circuit")):
                with TaskGroup("skip_graph", tooltip="Пропуск веток настоящими задачами графа"):
                    by = {o.group: o.loaded for o in STD_OPS}
                    from airflow.providers.standard.operators.empty import EmptyOperator
                    br = by["branch_python"](task_id="branch", python_callable=_graph_branch)
                    sc = by["short_circuit"](task_id="short", python_callable=lambda: False)
                    keep, drop, after = (EmptyOperator(task_id=x) for x in ("keep", "drop", "after"))
                    chk = skip_graph_check()
                    systems_t >> [br, sc]
                    br >> [keep, drop]
                    sc >> after
                    [keep, drop, after] >> chk  # check — ALL_DONE: выполняется и после ☮️ веток
                    leaves.append(chk)
```
  `Param` `op_skip_graph` (секция «Стандартные операторы», description «пропуск веток в графе»).
  В `report`: `test_ids = sorted(tid for tid in dag_.task_ids if tid.startswith(TOPS) and tid not in GRAPH_TASKS)`.
  Если `branch_python`/`short_circuit` не созданы — в сводке и `not_created` они уже есть; `skip_graph` не
  создаётся молча (её причина — их строка).
- [ ] ruff, EOL; стенд: прогон только `test_standart` → все ✅; у `branch_python` и `short_circuit` следующий
  тест группы выполнился (не ☮️); `task_sensor.timeout` длится 10–30 с; `skip_graph.check` ✅,
  `skip_graph.drop`/`after` ☮️; руками поменять `_graph_branch` на `return [SKIP_GRAPH + "keep", SKIP_GRAPH + "drop"]`
  на стенде → `check` ❌ (вернуть).
- [ ] Commit: `feat(test_hrp_operators): ветвление и @task.sensor в test_standart (REQ-hrp-regress-37, 38)`

**Критерии выхода:**
- На стенде 7 новых тестов ✅; ни одна задача регресса не ☮️ из-за ветвления (`report` выполнился).
- `grep -nE "deferrable=True|reschedule" tools/test_hrp_operators.py` — пусто.

---

### Задача 3.1: SQLExecuteQuery и ClickHouse (волна 3)

**REQ:** REQ-hrp-regress-40, 41, 37, 38, 36
**model-hint:** any
**Blocked by:** задача 2.1

**Контекст:** SQLExecuteQuery идёт через `PostgresHook` формы `pg` — во временном режиме его `get_conn`
отдаёт сессию теста (`T.pg_sessions(["main"])`, как у Hrp-групп). ClickHouse-операторы плагина 1.8.0:
`ClickHouseOperator`/`ClickHouseSensor` — native `ClickHouseHook` (`clickhouse_conn_id`),
`ClickHouseBranchSQLOperator` — dbapi (`conn_id`, `database`).

**Интерфейсы:**
- Потребляет: `_no_skip`, `_ids`, `_sensor_ok`, `_sensor_timeout` — задача 2.1; `T.pg_table`, `T.full`,
  `T.ch_table`, `T.ch_full`, `T.name`, `ROWS` — существующие.

**Шаги:**
- [ ] Тесты:
```python
# ─────────────────────────── Тесты: стандартные — SQL и CH ────────────────────────
_IDS = sorted(r[0] for r in ROWS)  # id — первая колонка COLUMNS


def _sql_select(t: T, sql, params, want):
    src = t.full(t.pg_table("src", ROWS))
    result, _ = t.run(conn_id=t.pg_conn, sql=sql.format(src=src), parameters=params, sessions=["main"])
    got = [tuple(r) for r in result or []]
    if got != want:
        raise AirflowFailException(f"получено {got}, ждём {want}")


def sql_execute_select(t: T):
    """SQLExecuteQueryOperator: SELECT с parameters по таблице теста на pg формы"""
    _sql_select(t, "SELECT id FROM {src} WHERE id <= %(n)s ORDER BY id", {"n": _IDS[1]}, [(i,) for i in _IDS[:2]])


def sql_execute_tpl(t: T):
    """SQLExecuteQueryOperator шаблоном: SQL из {{ }}"""
    _sql_select(t, "SELECT count(*) FROM {src} WHERE id <= {{{{ %d }}}}" % _IDS[1], None, [(2,)])


def ch_operator_rows(t: T):
    """ClickHouseOperator: DDL, INSERT … SELECT с parameters и SELECT одним списком"""
    src, tgt = t.ch_table("src", rows=ROWS), t.ch_full(t.name("tgt"))
    result, _ = t.run(clickhouse_conn_id=t.ch_conn, parameters={"n": _IDS[1]}, sql=[
        f"CREATE TABLE {tgt} (id Int64) ENGINE = MergeTree ORDER BY id",
        f"INSERT INTO {tgt} SELECT id FROM {src} WHERE id <= %(n)s",
        f"SELECT id FROM {tgt} ORDER BY id"])
    if [int(r[0]) for r in result] != _IDS[:2]:
        raise AirflowFailException(f"получено {result}, ждём {_IDS[:2]}")


def ch_operator_tpl(t: T):
    """ClickHouseOperator шаблоном: SQL из {{ }}"""
    result, _ = t.run(clickhouse_conn_id=t.ch_conn, sql="SELECT {{ 40 + 2 }}")
    if result != [(42,)]:
        raise AirflowFailException(f"получено {result}")


def ch_branch_sql_both(t: T):
    """ClickHouseBranchSQLOperator: обе ветки по результату SQL, задачи регресса не пропускаются"""
    for sql, want in (("SELECT 1", "systems"), ("SELECT 0", "report")):
        with _no_skip(t) as (m_all, _):
            t.run(conn_id=t.ch_conn, database=t.ch_db, sql=sql,
                  follow_task_ids_if_true="systems", follow_task_ids_if_false="report")
        asked = _ids(m_all.call_args.args[1]) if m_all.called else None
        if asked != [want]:
            raise AirflowFailException(f"{sql}: пропуск кроме {asked}, ждём {want}")


def ch_branch_sql_tpl(t: T):
    """ClickHouseBranchSQLOperator шаблоном: SQL из {{ }}"""
    with _no_skip(t) as (m_all, _):
        t.run(conn_id=t.ch_conn, database=t.ch_db, sql="SELECT {{ 1 }}",
              follow_task_ids_if_true="systems", follow_task_ids_if_false="report")
    if not m_all.called or _ids(m_all.call_args.args[1]) != ["systems"]:
        raise AirflowFailException(f"пропуск: {m_all.call_args}")


def ch_sensor_ok(t: T):
    """ClickHouseSensor шаблоном: строки есть — выход за первый poke"""
    src = t.ch_table("src", rows=ROWS)
    _sensor_ok(t, clickhouse_conn_id=t.ch_conn, sql=f"SELECT count() FROM {src} WHERE id > {{{{ 0 }}}}",
               is_success=lambda r: r[0][0] == len(ROWS))


def ch_sensor_timeout(t: T):
    """ClickHouseSensor: условие не выполнено — падение по timeout"""
    _sensor_timeout(t, clickhouse_conn_id=t.ch_conn, sql="SELECT 0", is_success=lambda r: bool(r[0][0]))
```
- [ ] В `STD_OPS`:
```python
    Op("sql_execute", "common.sql.operators.sql", "SQLExecuteQueryOperator", {"pg"},
       [sql_execute_select, sql_execute_tpl], pkg=STD),
    Op("ch_operator", "operators.clickhouse", "ClickHouseOperator", {"ch"},
       [ch_operator_rows, ch_operator_tpl], pkg="airflow_clickhouse_plugin"),
    Op("ch_branch_sql", "operators.clickhouse_dbapi", "ClickHouseBranchSQLOperator", {"ch"},
       [ch_branch_sql_both, ch_branch_sql_tpl], pkg="airflow_clickhouse_plugin"),
    Op("ch_sensor", "sensors.clickhouse", "ClickHouseSensor", {"ch"},
       [ch_sensor_ok, ch_sensor_timeout], pkg="airflow_clickhouse_plugin"),
```
- [ ] ruff, EOL; стенд: прогон `test_standart` в двух режимах `pg` — `airflowdb.hrp_test` и `airflowdb` →
  SQL/CH ✅; после рана `SELECT relname FROM pg_class WHERE relname LIKE 'rg\_sql\_execute%'` пусто,
  в CH `system.tables` нет `rg_ch_%`.
- [ ] Если `ClickHouseDbApiHook` не принимает подключение стенда (`conn_type`) — это находка регресса:
  тест ❌ с текстом ошибки, не обходить; вынести пользователю.
- [ ] Commit: `feat(test_hrp_operators): SQLExecuteQuery и ClickHouse в test_standart (REQ-hrp-regress-40, 41)`

**Критерии выхода:**
- На стенде 8 новых тестов ✅ в обоих режимах `pg`; объектов тестов после рана нет.

---

### Задача 4.1: S3 (волна 4)

**REQ:** REQ-hrp-regress-42, 38, 36
**model-hint:** any
**Blocked by:** задача 2.1

**Контекст:** бакеты не создаются: `S3CreateBucketOperator` идёт на бакет формы, `S3Hook.create_bucket`
подменён mock-ом — если оператор решит создавать, тест ❌, а бакет не появится.

**Интерфейсы:**
- Потребляет: `_sensor_ok`, `_sensor_timeout` — задача 2.1; `T.s3_put`, `T.s3_keys`, `T.s3_prefix`, `T.bucket`, `T.s3_conn`.

**Шаги:**
- [ ] Тесты:
```python
# ─────────────────────────────── Тесты: стандартные — S3 ──────────────────────────
def _s3_delete(t: T, **kw):
    for n in ("del/a.csv", "del/b.csv"):
        t.s3_put(n, b"x")
    t.run(aws_conn_id=t.s3_conn, bucket=t.bucket, **kw)
    if left := t.s3_keys("del/"):
        raise AirflowFailException(f"не удалены: {left}")


def s3_delete_keys(t: T):
    """S3DeleteObjectsOperator: удаление списком keys"""
    _s3_delete(t, keys=[t.s3_prefix + "del/a.csv", t.s3_prefix + "del/b.csv"])


def s3_delete_prefix(t: T):
    """S3DeleteObjectsOperator: удаление по prefix"""
    _s3_delete(t, prefix=t.s3_prefix + "del/")


def s3_delete_tpl(t: T):
    """S3DeleteObjectsOperator шаблоном: prefix из {{ }}"""
    _s3_delete(t, prefix=f"{{{{ {t.s3_prefix + 'del/'!r} }}}}")


def s3_key_sensor_ok(t: T):
    """S3KeySensor: ключ есть — выход за первый poke"""
    key = t.s3_put("ready.csv", b"x")
    _sensor_ok(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=key)


def s3_key_sensor_timeout(t: T):
    """S3KeySensor: ключа нет — падение по timeout"""
    _sensor_timeout(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=t.s3_prefix + "absent.csv")


def s3_key_sensor_tpl(t: T):
    """S3KeySensor шаблоном: bucket_key из {{ }}"""
    key = t.s3_put("ready.csv", b"x")
    _sensor_ok(t, aws_conn_id=t.s3_conn, bucket_name=t.bucket, bucket_key=f"{{{{ {key!r} }}}}")


def _s3_create_bucket(t: T, name):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    with mock.patch.object(S3Hook, "create_bucket") as create:  # бакеты регресс не создаёт
        t.run(aws_conn_id=t.s3_conn, bucket_name=name)
    if create.called or not t.s3.check_for_bucket(t.bucket):
        raise AirflowFailException(f"create_bucket вызван={create.called}: оператор не увидел бакет {t.bucket}")


def s3_create_bucket_exists(t: T):
    """S3CreateBucketOperator: бакет формы уже есть — без ошибки и без создания"""
    _s3_create_bucket(t, t.bucket)


def s3_create_bucket_tpl(t: T):
    """S3CreateBucketOperator шаблоном: bucket_name из {{ }}"""
    _s3_create_bucket(t, f"{{{{ {t.bucket!r} }}}}")
```
- [ ] В `STD_OPS`:
```python
    Op("s3_delete", "amazon.aws.operators.s3", "S3DeleteObjectsOperator", {"s3"},
       [s3_delete_keys, s3_delete_prefix, s3_delete_tpl], pkg=STD),
    Op("s3_key_sensor", "amazon.aws.sensors.s3", "S3KeySensor", {"s3"},
       [s3_key_sensor_ok, s3_key_sensor_timeout, s3_key_sensor_tpl], pkg=STD),
    Op("s3_create_bucket", "amazon.aws.operators.s3", "S3CreateBucketOperator", {"s3"},
       [s3_create_bucket_exists, s3_create_bucket_tpl], pkg=STD),
```
- [ ] ruff, EOL; стенд: 8 новых тестов ✅; список бакетов minio до и после рана одинаков
  (`ssh testsrv 'docker exec … mc ls'` или `S3Hook.list_buckets` в `python -c`).
- [ ] Commit: `feat(test_hrp_operators): S3-операторы в test_standart (REQ-hrp-regress-42)`

**Критерии выхода:**
- 8 тестов ✅; ключей под `<папка>/s3_*/` после рана нет; бакеты не изменились.

---

### Задача 5.1: TriggerDagRun, документация, полный ран (волна 5)

**REQ:** REQ-hrp-regress-43, 01, 35, 44
**model-hint:** any
**Blocked by:** задачи 3.1, 4.1

**Контекст:** в AF3 `TriggerDagRunOperator.execute` бросает `DagRunTriggerException`, ран создаёт
`task_runner._handle_trigger_dag_run` (сообщение супервизору). Тест ловит исключение, сверяет поля и
вызывает обработчик сам. Даг-цель — `tools_dummy` (ежечасный); нет ранов — значит нет дага → ☮️.

**Интерфейсы:**
- Потребляет: `T.run`, `STD_OPS` — задача 1.1.

**Шаги:**
- [ ] Тест:
```python
# ─────────────────────────── Тесты: стандартные — TriggerDagRun ───────────────────
DUMMY = "tools_dummy"


def trigger_dagrun_conf(t: T):
    """TriggerDagRunOperator шаблоном: ран tools_dummy с conf теста, без ожидания"""
    import structlog
    from airflow.sdk.exceptions import DagRunTriggerException
    from airflow.sdk.execution_time.task_runner import _handle_trigger_dag_run

    ti = t.context["ti"]
    # ponytail: «нет ранов» = «нет дага» — tools_dummy ежечасный; точнее — запрос DagBag через API
    if not ti.get_dr_count(dag_id=DUMMY):
        raise AirflowSkipException(f"нет дага {DUMMY} (нет ни одного рана)")
    run_id = f"{t.prefix}__{t.context['run_id']}__{ti.try_number}"[:250]
    try:
        t.run(trigger_dag_id=f"{{{{ {DUMMY!r} }}}}", trigger_run_id=run_id,
              conf={"rg": "{{ ti.task_id }}"}, wait_for_completion=False)
    except DagRunTriggerException as e:
        drte = e
    else:
        raise AirflowFailException("оператор не бросил DagRunTriggerException")
    if (drte.trigger_dag_id, drte.conf) != (DUMMY, {"rg": ti.task_id}):
        raise AirflowFailException(f"запрошено {drte.trigger_dag_id} conf={drte.conf}")
    _, state = _handle_trigger_dag_run(drte, t.context, ti, structlog.get_logger())
    if state is not None and str(state) != "success" or ti.get_dr_count(dag_id=DUMMY, run_ids=[run_id]) != 1:
        raise AirflowFailException(f"ран {DUMMY}/{run_id} не создан (state={state})")
```
  Перед реализацией прочитать на стенде `_handle_trigger_dag_run` целиком: что он возвращает при успехе
  (`state`), — и поправить условие проверки под факт.
- [ ] В `STD_OPS`:
```python
    Op("trigger_dagrun", "standard.operators.trigger_dagrun", "TriggerDagRunOperator", set(),
       [trigger_dagrun_conf], pkg=STD),
```
- [ ] Документация: `tools/readme.md` раздел test_hrp_operators — абзац о `test_standart` (состав 14, источник —
  замер сигмы 22.10, Kafka — в `test_kafka_*`, бакеты не создаются, ветвление подменено); `tools/skill/tools.md` —
  одна строка. Без упоминаний Claude/GitHub (feedback_docs_gigacode_only).
- [ ] ruff, EOL; стенд: **полный ран** (все группы, `pg=airflowdb.hrp_test`) → `report` success или ❌ только
  у известных Hrp-дефектов (сверить с последним раном v2.6); `test_standart` — 30 ✅ (`trigger_dagrun` ✅,
  у `tools_dummy` ран `rg_trigger_dagrun_conf__…` с conf); в форме секция «Стандартные операторы» с 14
  выключателями; Kafka-групп нет; снятый `op_ch_operator` → ☮️ «выключен».
- [ ] Commit: `feat(test_hrp_operators): TriggerDagRun в test_standart, readme (REQ-hrp-regress-43)`

**Критерии выхода:**
- Полный ран на стенде: 14 групп `test_standart`, 30 тестов ✅; `verdict` — прежние ключи, `groups` с новыми
  группами без префикса; `report` — блок «Стандартные операторы:».
- Дальше: ревью DeepSeek v4-pro + agy в фоне по спеке и диффу → свод `docs/sberpowers/reviews/2026-10-22-regress-standard-operators.md` → PR в `develop_3`.

### Задача 6.1: ещё 6 операторов по замеру dev (волна 6, поправка 22.10)

**REQ:** REQ-hrp-regress-34, 37, 41, 42, 46, 01
**model-hint:** any
**Blocked by:** задача 5.1

**Контекст:** замер сигмы dev за всё время (22.10) — сверх 14: `S3CopyObjectOperator` (20 задач),
`@task.branch` (3), `S3CreateObjectOperator` (1), `BranchSQLOperator` (1), `ClickHouseSQLExecuteQueryOperator` (1),
`HttpOperator` (2). Все в `ALLOWED_OPERATORS`. На стенде: `/api/v2/version` и `/api/v2/monitor/health` — 200 без
авторизации, `/api/v1` — 404 («removed in Airflow 3»); подключения `airflow_api` нет — его заведёт ядро
отдельной задачей. `ClickHouseSQLExecuteQueryOperator` подменён патчем ядра (как `ch_operator`) — на стенде
ожидается ❌ по тому же дефекту; логику теста проверить копией с исходным классом плагина.

**Шаги:**
- [ ] Тесты (после `ch_sensor_timeout` — CH/SQL, после `s3_create_bucket_tpl` — S3, после `trigger_dagrun_conf` — Http):
```python
def task_branch_both(t: T):
    """@task.branch: обе ветки по условию, задачи регресса не пропускаются"""
    for flag, want in ((True, "systems"), (False, "report")):
        _branch(t, flag, want, python_callable=_pick, op_kwargs={"flag": flag})


def task_branch_tpl(t: T):
    """@task.branch шаблоном: условие из {{ }} — строка 'True'"""
    _branch(t, "{{ 1 == 1 }}", "systems", python_callable=_pick, op_kwargs={"flag": "{{ 1 == 1 }}"})


def _branch_sql(t: T, sql: str, want: str, **kw):
    with _no_skip(t) as (m_all, _):
        t.run(conn_id=t.pg_conn, sql=sql, follow_task_ids_if_true="systems", follow_task_ids_if_false="report", **kw)
    asked = _ids(m_all.call_args.args[1]) if m_all.called else None
    if asked != [want]:
        raise AirflowFailException(f"{sql}: пропуск кроме {asked}, ждём {want}")


def branch_sql_both(t: T):
    """BranchSQLOperator: обе ветки по результату SQL с parameters на pg формы"""
    src = t.full(t.pg_table("src", ROWS))
    for n, want in ((_IDS[0], "systems"), (_IDS[0] - 1, "report")):
        _branch_sql(t, f"SELECT count(*) FROM {src} WHERE id <= %(n)s", want, parameters={"n": n}, sessions=["main"])


def branch_sql_tpl(t: T):
    """BranchSQLOperator шаблоном: SQL из {{ }}"""
    _branch_sql(t, "SELECT {{ 1 }}", "systems")


def ch_sql_execute_select(t: T):
    """ClickHouseSQLExecuteQueryOperator: SELECT с parameters по таблице теста"""
    src = t.ch_table("src", rows=ROWS)
    result, _ = t.run(conn_id=t.ch_conn, database=t.ch_db, parameters={"n": _IDS[1]},
                      sql=f"SELECT id FROM {src} WHERE id <= %(n)s ORDER BY id")
    if [int(r[0]) for r in result or []] != _IDS[:2]:
        raise AirflowFailException(f"получено {result}, ждём {_IDS[:2]}")


def ch_sql_execute_tpl(t: T):
    """ClickHouseSQLExecuteQueryOperator шаблоном: SQL из {{ }}"""
    result, _ = t.run(conn_id=t.ch_conn, database=t.ch_db, sql="SELECT {{ 40 + 2 }}")
    if [tuple(r) for r in result or []] != [(42,)]:
        raise AirflowFailException(f"получено {result}")


def _s3_copy(t: T, dst: str):
    src = t.s3_put("src.csv", b"id\n1\n")
    t.run(aws_conn_id=t.s3_conn, source_bucket_name=t.bucket, source_bucket_key=src,
          dest_bucket_name=t.bucket, dest_bucket_key=dst)
    if (got := t.s3.read_key(t.s3_prefix + "dst.csv", t.bucket)) != "id\n1\n":
        raise AirflowFailException(f"в копии {got!r}")


def s3_copy_key(t: T):
    """S3CopyObjectOperator: копия ключа теста в своей папке"""
    _s3_copy(t, t.s3_prefix + "dst.csv")


def s3_copy_tpl(t: T):
    """S3CopyObjectOperator шаблоном: dest_bucket_key из {{ }}"""
    _s3_copy(t, f"{{{{ {t.s3_prefix + 'dst.csv'!r} }}}}")


def _s3_create_object(t: T, data, want):
    key = t.s3_prefix + "obj.csv"
    t.run(aws_conn_id=t.s3_conn, s3_bucket=t.bucket, s3_key=key, data=data, replace=True)
    if (got := t.s3.read_key(key, t.bucket)) != want:
        raise AirflowFailException(f"записано {got!r}, ждём {want!r}")


def s3_create_object_data(t: T):
    """S3CreateObjectOperator: ключ из data"""
    _s3_create_object(t, "id\n1\n", "id\n1\n")


def s3_create_object_tpl(t: T):
    """S3CreateObjectOperator шаблоном: data из {{ }}"""
    _s3_create_object(t, "{{ 40 + 2 }}", "42")


HTTP_CONN = "airflow_api"  # свой API контура; заводит ядро


def _http(t: T, endpoint: str):
    from airflow.sdk import BaseHook
    try:
        BaseHook.get_connection(HTTP_CONN)
    except Exception:
        raise AirflowSkipException(f"нет подключения {HTTP_CONN}") from None
    import airflow
    result, _ = t.run(http_conn_id=HTTP_CONN, method="GET", endpoint=endpoint, response_filter=lambda r: r.json())
    if (result or {}).get("version") != airflow.__version__:
        raise AirflowFailException(f"ответ {result}, ждём версию {airflow.__version__}")


def http_version(t: T):
    """HttpOperator: GET api/v2/version своего API, версия = версии рана"""
    _http(t, "api/v2/version")


def http_tpl(t: T):
    """HttpOperator шаблоном: endpoint из {{ }}"""
    _http(t, "{{ 'api/v2/version' }}")
```
- [ ] В `STD_OPS`: `task_branch` (`standard.decorators.branch_python`, `_BranchPythonDecoratedOperator`, set()),
  `branch_sql` (`common.sql.operators.sql`, `BranchSQLOperator`, {"pg"}), `ch_sql_execute`
  (`clickhouse_dbapi`, `ClickHouseSQLExecuteQueryOperator`, {"ch"}, pkg `airflow_clickhouse_plugin`), `s3_copy`,
  `s3_create_object` (`amazon.aws.operators.s3`, {"s3"}), `http` (`http.operators.http`, `HttpOperator`, set()).
  Модуль/класс сверить на стенде до правки.
- [ ] Документация: readme — 14 → 20 и новые имена, Http — подключение `airflow_api`; `tools/skill/tools.md` — 14 → 20.
- [ ] ruff, EOL; стенд: прогон новых групп; `http` с подключением `airflow_api` (вручную: `airflow connections add
  airflow_api --conn-type http --conn-host localhost --conn-port 8093 --conn-schema http`) → ✅, удалить
  подключение → ☮️ «нет подключения»; `ch_sql_execute` — ❌ по патчу ядра, логика ✅ копией с исходным классом;
  полный ран → 60 Hrp ✅ как v2.6, ❌ только ch_operator/ch_branch_sql/ch_sql_execute.
- [ ] Commit: `feat(test_hrp_operators): ещё 6 стандартных операторов по замеру dev (REQ-hrp-regress-46)`

**Критерии выхода:** 20 групп `test_standart`, 42 теста; полный ран — ❌ только известные ядровые; `verdict` — прежние ключи.

## Self-review
- Покрытие: 01 — 1.1, 5.1; 34 — 1.1–5.1; 35 — 1.1, 5.1; 36 — 1.1–4.1; 37 — 2.1, 3.1; 38 — 2.1, 3.1, 4.1;
  39 — 1.1; 40 — 3.1; 41 — 3.1, 6.1; 42 — 4.1, 6.1; 43 — 5.1; 44 — 1.1, 5.1; 45 — 2.1; 46 — 6.1;
  34, 37, 01 — ещё и 6.1.
- Счёт тестов: 1.1 — 5, 2.1 — 7 + check, 3.1 — 8, 4.1 — 8, 5.1 — 1 → 30; 6.1 — 12 → 42.
- Имена: `_no_skip`, `_ids`, `_sensor_ok`, `_sensor_timeout`, `SENSOR_TIMEOUT`, `STD`, `STD_OPS`, `TOPS` — одинаковы во всех задачах.
- Риски на стенде: `ClickHouseDbApiHook` и тип подключения `dlab-click`; ответ `_handle_trigger_dag_run`;
  `SQLExecuteQueryOperator` во временном режиме (hook.run открывает соединение через `get_conn` — подменён).
