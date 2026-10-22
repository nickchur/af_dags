# Delta-спека: стандартные операторы в регрессе test_hrp_operators (AF3)

- Дата: 2026-10-22
- Статус: подтверждена пользователем 22.10.2026
- Baseline: спека `2026-10-16-test-hrp-operators-groups` (REQ-hrp-regress-01…33), точка отсчёта — `test_hrp_operators` v2.6 в `develop_3`
- Источник: HRPDATALAB-14480 «DAGS | Тесты используемых операторов для регресса» (эпик AirFlow Q42026, КЭ DataPlatform-DAGs 6932748); замер `task_instance` сигмы за 7 дней (22.10); решения пользователя 22.10: группы в том же даге, все операторы из замера, SQLExecuteQuery — на PG/GP формы, Kafka/TriggerDagRun/S3CreateBucket — безопасно

## Мотивация

Регресс перед релизом и хотфиксом (встраивается в пайплайн DPM) и при обновлении Airflow и библиотек
проверяет только `hrp_operators`. При этом на сигме больше всего задач у стандартных операторов
Airflow и провайдеров: `ClickHouseOperator` используется в 169 дагах, `PythonOperator` в 62. Их поломка
после обновления провайдера сейчас гейтом не ловится. Тег `AutoQA` у `test_connections` и
`test_hrp_operators` уже стоит, `show_connections` слит в `test_connections`, так что в этой части
тикет закрыт без правок.

## ADDED — новое поведение

- **REQ-hrp-regress-34** — **Стандартные операторы.** К группам регресса добавляются 14 групп для
  стандартных операторов из замера сигмы: `ClickHouseOperator`, `ClickHouseBranchSQLOperator`,
  `ClickHouseSensor`, `EmptyOperator`, `PythonOperator`, `BranchPythonOperator`, `ShortCircuitOperator`,
  `@task` (`_PythonDecoratedOperator`), `@task.sensor` (`DecoratedSensorOperator`),
  `SQLExecuteQueryOperator`, `S3DeleteObjectsOperator`, `S3KeySensor`, `S3CreateBucketOperator`,
  `TriggerDagRunOperator`. Класс каждой группы берётся из пакета, в котором его импортируют даги
  контура (провайдеры `standard`, `common.sql`, `amazon`, плагин `airflow_clickhouse_plugin`).
- **REQ-hrp-regress-44** — **Своя главная группа.** Группы стандартных операторов лежат в отдельной
  главной группе `test_standart` (`task_id = test_standart.<группа>.<тест>`). Hrp-группы остаются в
  `tests`, и их `task_id` не меняются. Имена групп уникальны в обеих главных группах. В `verdict`
  и в сводке `report` имена групп идут без префикса главной группы, как сейчас. Сводка `report`
  показывает стандартные операторы отдельным блоком.
- **REQ-hrp-regress-35** — **Те же правила, что у Hrp-групп.** На новые группы распространяются
  REQ-hrp-regress-02…09, 16, 17 и 20…27: отдельная группа на оператор, правила «нет в пакете / запрещён /
  сломан», выключатель `op_<группа>`, выключатели систем, подготовка и уборка внутри теста, Jinja и
  `task_policy` как в бою, порядок, `report` и `verdict`. Ключи `verdict` не меняются. Новые группы
  появляются в `groups` и `not_created` наравне с прочими.
- **REQ-hrp-regress-36** — **Шаблонный случай — там, где есть что шаблонировать.** REQ-hrp-regress-18
  действует для новых групп, у оператора которых непустые `template_fields`. Ключевое поле (`sql`,
  `bucket_name`/`keys`/`bucket_key`, `op_kwargs`, `trigger_dag_id`/`conf`) передаётся шаблоном
  `{{ … }}`, и тест сверяет, что подействовало отрендеренное значение. У `EmptyOperator` шаблонного
  случая нет.
- **REQ-hrp-regress-37** — **Ветвление не задевает граф регресса.** Тесты `BranchPythonOperator`,
  `ShortCircuitOperator` и `ClickHouseBranchSQLOperator` проверяют, какую ветку (или «продолжать / не
  продолжать») выбрал оператор, на обоих исходах условия. При этом ни одна задача дага регресса не
  пропускается из-за решения проверяемого оператора: следующий тест группы и `report` выполняются.
- **REQ-hrp-regress-45** — **Настоящий пропуск веток.** Кроме тестов из REQ-hrp-regress-37, где пропуск
  подменён, в `test_standart` есть подгруппа `skip_graph`. В ней `BranchPythonOperator` и
  `ShortCircuitOperator` — настоящие задачи графа, у каждого есть свои downstream-задачи. Тест
  `skip_graph.check` проверяет по состояниям рана: выбранная ветка выполнена, невыбранная и задача после
  «нет» у ShortCircuit — ☮️. Если это не так, тест ❌ и ран красный. В `verdict` и сводке учитывается только
  `check`, служебные задачи подгруппы в счёт не идут. Подгруппы нет, если любой из двух операторов не
  создан (нет в пакете или запрещён). Выключатель — `op_skip_graph`.
- **REQ-hrp-regress-38** — **Сенсоры: успех и тайм-аут.** Тесты `S3KeySensor`, `ClickHouseSensor` и
  `@task.sensor` проверяют два исхода. Условие выполнено — сенсор завершается без ошибки за первый poke.
  Условие не выполнено — сенсор падает по своему `timeout` (не больше 30 с), и тест засчитывает это как
  ✅. Режим — `poke`, без deferrable.
- **REQ-hrp-regress-39** — **Python-семейство.** Тесты `PythonOperator` и `@task` сверяют возврат
  функции, переданные `op_args`/`op_kwargs` и то, что функция получила контекст рана (`ds`/`ti`).
  `EmptyOperator` выполняется без ошибки.
- **REQ-hrp-regress-40** — **SQL на PG/GP формы.** Тест `SQLExecuteQueryOperator` выполняется на
  подключении `pg` формы в обоих режимах (схема и временные таблицы, REQ-hrp-regress-10…12). Он создаёт
  и наполняет таблицу теста и сверяет результат `SELECT` (строки и `row_count`, если он есть) и
  применение `parameters`. На ClickHouse этот оператор не проверяется.
- **REQ-hrp-regress-41** — **ClickHouseOperator.** Тест выполняет DDL, `INSERT` и `SELECT` на `ch` формы
  и сверяет возвращённые строки с вставленными, а также применение `parameters`.
- **REQ-hrp-regress-42** — **S3 без создания бакетов.** `S3CreateBucketOperator` вызывается на бакете
  `s3` формы, который уже существует. Тест проходит, если оператор завершился без ошибки, а бакет
  остался на месте; новых бакетов не появляется. `S3DeleteObjectsOperator` удаляет ключи теста (и
  списком `keys`, и по `prefix`), и тест сверяет, что их больше нет. `S3KeySensor` следует REQ-hrp-regress-38.
- **REQ-hrp-regress-43** — **TriggerDagRun без ожидания.** Тест `TriggerDagRunOperator` запускает даг
  `tools_dummy` с `conf` теста и `wait_for_completion=False`, а затем сверяет, что в метабазе появился ран
  `tools_dummy` с этим `conf` и `run_id` теста. Если дага `tools_dummy` на контуре нет, тест завершается ☮️ с
  причиной, и ран из-за этого не краснеет.

## MODIFIED — изменённое поведение

- **REQ-hrp-regress-01** — было: 24 группы операторов `hrp_operators` + lineage → станет: те же 24 + 14
  групп стандартных операторов (REQ-hrp-regress-34) + lineage. Исключения прежние (Idp, Datacatalog,
  LLM, Embeddings, OpenMetadata), к ним добавлены `ConsumeFromTopicOperator`/`ProduceToTopicOperator`.
  Kafka покрывает отдельный даг `test_kafka_*`.

## REMOVED — удаляемое поведение

Нет.

## Ограничения и контракты

- Гейт DPM: `dag_id=test_hrp_operators`, задача `report`, ключи `verdict` не меняются. Имя дага остаётся
  прежним, хотя в нём теперь не только Hrp-операторы: переименование сломало бы контракт DPM.
- Системы новых групп: CH-операторы — `ch`, SQLExecuteQuery — `pg`, S3-операторы — `s3`. Python, Empty
  и TriggerDagRun внешних систем не требуют.
- Оператор выполняется внутри задачи-теста, как Hrp-операторы. В AF3 ветвление (`skip_all_except`) и
  `TriggerDagRunOperator` (`DagRunTriggerException`) реализованы через исключения, которые
  перехватывает исполнитель задачи. Как тест ловит такие исключения и сверяет их, не отдавая
  исполнителю, решает plan. Ограничения REQ-hrp-regress-37 и 43 обязательны.
- Нет Triggerer: deferrable и `wait_for_completion=True` не используются.
- `ch` в форме — подключение для native-протокола `airflow_clickhouse_plugin`, тот же `ch`, что у
  Hrp-групп.
- Python 3.10, AF3 (`airflow.sdk`). Версия в docstring модуля поднимается по правилу репозитория. readme
  и навык `tools` обновляются под новый состав.

## Non-goals

- `ConsumeFromTopicOperator` и `ProduceToTopicOperator`: их покрывает `test_kafka_*`.
- `SQLExecuteQueryOperator` на ClickHouse и прочих базах: только PG/GP формы.
- Создание и удаление новых бакетов S3.
- `wait_for_completion`, deferrable-режимы и `reschedule`-режим сенсоров.
- Операторы из белого списка, которых нет в замере сигмы (HttpOperator, SQL*Check, ExternalTaskSensor
  и др.): добавляются по следующему замеру отдельным изменением.
- Пользовательские обёртки из чужих дагов (`LoggedHrp…`) не проверяются.
- AF2 (`master`) не трогается.

## Критерии приёмки

| REQ-ID | Сценарий проверки |
|---|---|
| REQ-hrp-regress-34 | разобрать даг на стенде AF3 → 14 новых групп с именами операторов |
| REQ-hrp-regress-44 | граф → новые группы внутри `test_standart`, `task_id` Hrp-тестов прежние (`tests.…`); `verdict.groups` — имена без префикса; в заметке `report` отдельный блок стандартных |
| REQ-hrp-regress-35 | снять `op_<новая группа>` → её тесты ☮️ «выключен»; убрать класс из `ALLOWED_OPERATORS` → группы нет, в `not_created` «запрещён»; `verdict.groups` содержит новые группы |
| REQ-hrp-regress-36 | у каждой новой группы, кроме `empty`, есть шаблонный тест; подмена отрендеренного значения на другое (руками в стенде) → тест ❌ |
| REQ-hrp-regress-37 | полный ран → в группах ветвления оба исхода ✅, ни одна задача регресса не ☮️ по причине «skipped by branch / short-circuit» |
| REQ-hrp-regress-45 | ран → `skip_graph.check` ✅, `drop` и `after` ☮️, `keep` ✅; подменить в стенде возврат ветки на несуществующую/обе → `check` ❌; убрать `ShortCircuitOperator` из `ALLOWED_OPERATORS` → подгруппы нет, даг разбирается |
| REQ-hrp-regress-38 | ран → у каждого сенсора тест «есть» ✅ за один poke, тест «нет» ✅, длительность ≤ timeout + запас; в коде нет `deferrable=True`, `mode="reschedule"` |
| REQ-hrp-regress-39 | ран → Python/@task ✅; намеренно неверный ожидаемый возврат → ❌ с обоими значениями |
| REQ-hrp-regress-40 | `pg=conn.схема` и `pg=conn` → SQLExecuteQuery ✅ в обоих; после рана объектов теста нет |
| REQ-hrp-regress-41 | ран → ClickHouseOperator ✅, после рана таблиц теста в `ch` нет |
| REQ-hrp-regress-42 | ран → S3CreateBucket ✅, список бакетов подключения до и после одинаков; S3Delete ✅, ключей теста нет |
| REQ-hrp-regress-43 | ран → у `tools_dummy` новый ран с `conf` теста; на стенде без `tools_dummy` → ☮️, `report` зелёный |
| REQ-hrp-regress-01 | разобрать даг → 24 Hrp + 14 стандартных + lineage, групп Kafka нет |

## Traceability

| REQ-ID | Задачи плана | Тесты | Статус |
|---|---|---|---|
| REQ-hrp-regress-01 | 1.1, 5.1 | — | не начато |
| REQ-hrp-regress-34 | 1.1–5.1 | — | не начато |
| REQ-hrp-regress-35 | 1.1, 5.1 | — | не начато |
| REQ-hrp-regress-36 | 1.1–4.1 | — | не начато |
| REQ-hrp-regress-37 | 2.1, 3.1 | — | не начато |
| REQ-hrp-regress-38 | 2.1, 3.1, 4.1 | — | не начато |
| REQ-hrp-regress-39 | 1.1 | — | не начато |
| REQ-hrp-regress-40 | 3.1 | — | не начато |
| REQ-hrp-regress-41 | 3.1 | — | не начато |
| REQ-hrp-regress-42 | 4.1 | — | не начато |
| REQ-hrp-regress-43 | 5.1 | — | не начато |
| REQ-hrp-regress-44 | 1.1, 5.1 | — | не начато |
| REQ-hrp-regress-45 | 2.1 | — | не начато |

## Задачи

- План: docs/sberpowers/plans/2026-10-22-regress-standard-operators.md (задачи 1.1, 2.1, 3.1, 4.1, 5.1)
