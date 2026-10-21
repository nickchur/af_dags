# План: загрузчик берёт из CTL только своё поддерево сущностей

> Исполняется скиллом execute. Исполнитель — я (правило с 13.10.2026), ревью — DeepSeek v4-pro и agy
> параллельно в фоне. Шаги — чекбоксы `- [ ]`.

**Спека:** docs/sberpowers/specs/2026-10-21-loader-entities-subtree.md (REQ-ctl-worker-39, REQ-ctl-worker-35, REMOVED)
**Цель:** `CTL.<профиль>.loader` не запрашивает и не хранит полный реестр сущностей CTL; поддерево — одним
запросом `child?direct=false`, существование чужих сущностей — адресно.
**Архитектура:** две чистые функции в `ctl_worker/ctl_utils.py` (разбор дерева, проверка существования) с
тестами; `load_entities`/`load_events` в `ctl_worker/ctl_loader.py` переходят на них; заглушка CTL стенда
отдаёт ту же форму ответа, что боевой CTL.

## Глобальные ограничения
- Ветка `fix/loader-entities-subtree` от `develop_3` (AF3); AF2 (`master`) не трогаем.
- Python ≥ 3.10; окончания строк LF (`git ls-files --eol`).
- Строка версии модуля — 2-я строка docstring: `TZ=Europe/Moscow date '+%Y-%m-%d %H:%M'`, версия +1, автор
  `Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)`.
- `ruff check` до коммита. Тесты: `python -m pytest tests -q` в `/opt/aftest/venv312` на testsrv.
- Форма ответа `child?direct=false` у боевого CTL: `{"entity": {...}, "kidz": [{"entity": {...}, "kidz": [...]}]}`,
  корень входит в ответ.

## Волны
- Волна 0: задача 0 (гейт на контуре, делает пользователь) — блокирует только 1.2 (правило «4xx — нет сущности»)
- Волна 1: задача 1.1 — без блокеров
- Волна 2: задача 1.2 (blocked by 0, 1.1), задача 2.1 (blocked by 1.1) — разные файлы и репо, параллельны
- Волна 3: задача 3.1 (blocked by 1.2, 2.1)

---

### Задача 0: Гейт — ответ CTL на несуществующую сущность (волна 0)

**REQ:** REQ-ctl-worker-35
**Blocked by:** нет блокеров

Пользователь запускает `tools_ctl_check_api` на альфе dev дважды:
- `url` = `/v4/api/entity/999999999` → **ждём 4xx** (404/422) и строку `❌ Error: 4..` в логе задачи;
- `url` = `/v4/api/entity/941000000` → **ждём 200** и запись `{"id": 941000000, "name": "HR", ...}`.

**Стоп-критерий:** несуществующая сущность даёт 200 (пустое тело, `null`, `{}`) — правило «4xx = нет» неверно;
возвращаемся в specify: признак отсутствия — пустой ответ.

---

### Задача 1.1: Разбор дерева и проверка существования сущностей (волна 1)

**REQ:** REQ-ctl-worker-39, REQ-ctl-worker-35
**model-hint:** any
**Blocked by:** нет блокеров

**Файлы:**
- Изменить: `ctl_worker/ctl_utils.py` — новые функции после `ctl_obj_load`; строка версии v1.15 → v1.16
- Тест: `tests/test_ctl_entities.py`

**Интерфейсы — производит:**
- `ctl_tree_entities(tree, root) -> dict[int, dict]` — для задачи 1.2
- `ctl_entity_exists(eid) -> bool | None` — True есть, False 4xx (нет), None сбой
- `ctl_entities_present(needed, known) -> set[int]` — для задачи 1.2

**Шаги:**
- [ ] Тест `tests/test_ctl_entities.py`:
```python
"""Поддерево сущностей из child?direct=false и адресная проверка (REQ-ctl-worker-39/35).

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""

from types import SimpleNamespace

import pytest
import requests
from airflow.sdk.exceptions import AirflowSkipException

from ctl_worker import ctl_utils


def node(eid, *kids, parent=0):
    return {'entity': {'id': eid, 'name': f'e{eid}', 'parentId': parent}, 'kidz': list(kids)}


TREE = node(10, node(30, node(31, parent=30), parent=10), node(20, parent=10))


def test_tree_bfs_sorted_with_root():
    out = ctl_utils.ctl_tree_entities(TREE, 10)
    assert list(out) == [10, 20, 30, 31]
    assert out[31] == {'id': 31, 'name': 'e31', 'parentId': 30}


def test_tree_order_independent_of_response():
    shuffled = node(10, node(20, parent=10), node(30, node(31, parent=30), parent=10))
    assert list(ctl_utils.ctl_tree_entities(shuffled, 10)) == list(ctl_utils.ctl_tree_entities(TREE, 10))


@pytest.mark.parametrize('tree', [None, [], {}, {'entity': {'id': 99}, 'kidz': []}])
def test_tree_without_root_is_empty(tree):
    assert ctl_utils.ctl_tree_entities(tree, 10) == {}


@pytest.fixture
def ctl(monkeypatch):
    calls = []
    answers = {}

    def fake(url, **kw):
        calls.append((url, kw))
        a = answers.get(url, {'id': 1})
        if isinstance(a, BaseException):
            raise a
        return a

    monkeypatch.setattr(ctl_utils, 'ctl_api', fake)
    return SimpleNamespace(calls=calls, answers=answers)


def test_exists_true_without_gp_log(ctl):
    assert ctl_utils.ctl_entity_exists(5) is True
    assert ctl.calls == [('/v4/api/entity/5', {'gp_log': False})]


def test_exists_false_on_4xx(ctl):
    ctl.answers['/v4/api/entity/5'] = AirflowSkipException('404')
    assert ctl_utils.ctl_entity_exists(5) is False


def test_exists_none_on_failure(ctl):
    ctl.answers['/v4/api/entity/5'] = requests.exceptions.ReadTimeout('t')
    assert ctl_utils.ctl_entity_exists(5) is None


def test_present_known_without_requests(ctl):
    assert ctl_utils.ctl_entities_present({1, 2}, {1, 2, 3}) == {1, 2, 3}
    assert ctl.calls == []


def test_present_outside_once_and_by_answer(ctl):
    ctl.answers['/v4/api/entity/7'] = AirflowSkipException('404')
    ctl.answers['/v4/api/entity/8'] = requests.exceptions.ConnectionError('c')
    got = ctl_utils.ctl_entities_present([1, 6, 6, 7, 8], {1})
    assert got == {1, 6, 8}            # 7 — нет в CTL; 8 — сбой, висячей не считаем
    assert [u for u, _ in ctl.calls] == ['/v4/api/entity/6', '/v4/api/entity/7', '/v4/api/entity/8']


def test_ctl_api_maps_404_to_skip(monkeypatch):
    """Настоящий ctl_api: 404 у CTL → AirflowSkipException → ctl_entity_exists() is False."""
    resp = SimpleNamespace(status_code=404, text='404 Entity not found')

    class Hook:
        def __init__(self, **kw):
            pass

        def run(self, **kw):
            raise requests.exceptions.HTTPError(response=resp)

    monkeypatch.setattr(ctl_utils, 'KerberosHttpHook', Hook)
    monkeypatch.setattr(ctl_utils, 'rate_limit', lambda: None)
    monkeypatch.setattr(ctl_utils, 'get_config', lambda: {})
    monkeypatch.setattr(ctl_utils, 'add_note', lambda *a, **k: None)
    assert ctl_utils.ctl_entity_exists(5) is False
```
- [ ] Прогон на стенде — ожидаем FAIL `AttributeError: module 'ctl_worker.ctl_utils' has no attribute 'ctl_tree_entities'`
- [ ] Реализация в `ctl_utils.py` (после `ctl_obj_load`):
```python
def ctl_tree_entities(tree, root):
    """Сущности поддерева root из ответа `/v4/api/entity/{root}/child?direct=false`: {id: entity}.

    Ответ CTL — дерево `{entity, kidz}` вместе с самим корнем. Порядок фиксирован — в ширину,
    дети по возрастанию id: на нём стоит сравнение MD5 в ctl_obj_save, и от порядка ответа он
    зависеть не должен. Корня в ответе нет — пусто.
    """
    flat, kids = {}, {}
    stack = [tree] if isinstance(tree, dict) else []
    while stack:
        n = stack.pop()
        ent = n.get('entity') or {}
        if ent.get('id') is None:
            continue
        eid = int(ent['id'])
        flat[eid] = ent
        for k in n.get('kidz') or []:
            kid = (k.get('entity') or {}).get('id')
            if kid is not None:
                kids.setdefault(eid, []).append(int(kid))
                stack.append(k)

    out, queue = {}, [int(root)]
    while queue:
        eid = queue.pop(0)
        if eid in out or eid not in flat:
            continue
        out[eid] = flat[eid]
        queue.extend(sorted(kids.get(eid, [])))
    return out


def ctl_entity_exists(eid):
    """Есть ли сущность в CTL: True — есть, False — CTL ответил 4xx, None — не ответил по существу.

    Журнал GET в GP не пишем: это проверка, а не данные.
    """
    try:
        ctl_api(f'/v4/api/entity/{int(eid)}', gp_log=False)
        return True
    except AirflowSkipException:
        return False
    except Exception as e:
        logger.warning(f"⚠️ Сущность {eid}: CTL не ответил ({e}) — висячей не считаем")
        return None


def ctl_entities_present(needed, known):
    """Какие сущности есть в CTL: known — без запросов, остальные из needed — адресно, по разу.

    Сбой проверки (None) считаем «есть»: сенсор лучше спросит лишний раз, чем потеряет событие.
    """
    known = {int(e) for e in known}
    return known | {e for e in sorted({int(e) for e in needed} - known) if ctl_entity_exists(e) is not False}
```
- [ ] Строка версии `ctl_utils.py`: v1.15 → v1.16, время MSK
- [ ] Прогон на стенде (копия ветки `scp` со сверкой md5) — PASS; `ruff check ctl_worker tests` — чисто
- [ ] Commit: `feat(ctl_utils): поддерево сущностей из child и адресная проверка (REQ-ctl-worker-39/35)`

**Критерии выхода:** `python -m pytest tests -q` — все зелёные, включая старые `test_ctl_api_gp_pool.py`.

---

### Задача 1.2: Загрузчик на новые функции (волна 2)

**REQ:** REQ-ctl-worker-39, REQ-ctl-worker-35, REMOVED (`ctl_entities_all`, счёт «вне поддерева»)
**model-hint:** any
**Blocked by:** задача 0, задача 1.1

**Файлы:** `ctl_worker/ctl_loader.py` (v1.11 → v1.12), `ctl_worker/readme.md` (абзац про `/v4/api/entity`).

**Интерфейсы — потребляет:** `ctl_tree_entities(tree, root)`, `ctl_entities_present(needed, known)` — задача 1.1.

**Шаги:**
- [ ] Импорт: `from ctl_worker.ctl_utils import get_config, ctl_obj_save, ctl_obj_load, ctl_api, ctl_tree_entities, ctl_entities_present`
- [ ] Удалить вложенную `entity_subtree` (её единственный вызов — в `load_entities`).
- [ ] Тело `load_entities` после docstring:
```python
        # Поддерево одним запросом «все потомки корня», а не весь реестр `/v4/api/entity`: тот на
        # альфе — 402 477 сущностей и 57 МБ JSON ради 694 наших; разбор, журнал и сохранение давали
        # ~0,8 ГБ на задачу, и 21.10.2026 воркеры с лимитом 3 GiB убивало при старте загрузчика.
        # Поисковую `/entity/tree?search=` не берём: 17.09.2026 она теряла свежие сущности.
        # `child?direct=false` 21.10.2026 сверен с реестром: 716 из 716 узлов, поля совпадают.
        root = int(get_config()['root_entity'])
        eids = ctl_tree_entities(ctl_api(f'/v4/api/entity/{root}/child?direct=false'), root)
        if not eids:
            # Пустое поддерево — сбой ответа, а не правда: не затираем ctl_entities, задача повторится
            raise Exception(f'❌ CTL не вернул поддерево {root}')
        add_note(f"🌳 поддерево {root}: {len(eids)}", context, level='DAG,Task')
        load_obj_save('ctl_entities', eids, var=True, skip=True)
```
- [ ] В `load_events` вместо блока `ctl_entities_all` (комментарий + две строки `data = …`):
```python
        # Сущность события есть в CTL, если она в нашем поддереве; чужие спрашиваем адресно, по разу.
        present = ctl_entities_present(entity_events, eids)
        logger.info(f"сущностей событий вне поддерева: {len(entity_events - set(eids))}")
```
  и вызов `dangling_events(events, sources, present, ctl_profile_names())`.
- [ ] Docstring `dangling_events`: «Сущности нет в CTL (`entities` — множество существующих id) или профиля нет в `/v4/api/profile` …».
- [ ] `readme.md`: абзац про обход `/v4/api/entity` заменить на «поддерево — `/v4/api/entity/{root}/child?direct=false`;
  поиск по дереву не годится (индекс отстаёт); полный реестр не запрашивается — 402 тыс. сущностей валили воркер по памяти».
- [ ] `grep -rn "ctl_entities_all\|entity_subtree" ctl_worker tools` — пусто; `ruff check`; тесты стенда зелёные.
- [ ] Commit: `fix(ctl_loader): поддерево сущностей без полного реестра CTL (REQ-ctl-worker-39/35)`

**Критерии выхода:** grep пуст; даг импортируется на стенде без ошибок (`list_import_errors` MCP стенда пуст).

---

### Задача 2.1: Заглушка CTL стенда — форма ответа как у боевого (волна 2)

**REQ:** REQ-ctl-worker-39, REQ-ctl-worker-35 (стендовая проверка)
**Blocked by:** задача 1.1

**Файлы:** testsrv `/opt/aftest/ctl-mock3/ctl_mock.py` (вне репо; бэкап `ctl_mock.py.bak-20261021`).

- [ ] `entity_child` при `direct=false` возвращает `entity_tree(eid)` (та же форма `{entity, kidz}`, что у
  поиска), при `direct=true` — как раньше.
- [ ] `entity_one`: сущность ищется в `ENTITIES_ALL`; нет — `JSONResponse({'message': f'Entity {eid} not found'}, status_code=404)`.
- [ ] `systemctl restart ctl-mock3`; `curl -s localhost:<порт>/v4/api/entity/<root>/child?direct=false | head -c 300` — дерево;
  `curl -s -o /dev/null -w '%{http_code}' …/v4/api/entity/999999999` — `404`.

---

### Задача 3.1: Стенд — прогон загрузчика (волна 3)

**REQ:** все
**Blocked by:** задача 1.2, задача 2.1

- [ ] Ветку на стенд AF3 (`scp` + md5), дождаться разбора.
- [ ] Триггер `CTL.HR_Data.loader`; `load_entities` — success, заметка «🌳 поддерево …: N» (N = числу узлов
  фикстуры); `ctl_entities` в Variable не изменился по составу против прошлого прогона.
- [ ] `load_events` — success; с `CTL_MOCK_MISSING_ENTITIES` из env заглушки висячие ключи в заметке с причиной
  «сущности N нет в CTL»; в логе `сущностей событий вне поддерева: K`.
- [ ] В логах задач нет `GET /v4/api/entity` без id и нет `ctl_entities_all`.
- [ ] Ревью: DeepSeek v4-pro и agy в фоне по спеке + `git diff origin/develop_3...HEAD`; свод в
  `docs/sberpowers/reviews/2026-10-21-loader-entities-subtree.md`.

**Приёмка на альфе после выкладки (спека):** заметка `load_entities` ≈ 694; `LastModified` у
`ctl_entities_all` в S3 замер; за сутки нет перезапусков воркеров на старте загрузчика; новая сущность в
поддереве видна в ближайший прогон.

### Задача 4.1: Проверка ссылок в экспорте yml (добавлена 21.10.2026 по запросу пользователя)

**REQ:** REQ-ctl-worker-63
**Blocked by:** нет блокеров

- [ ] В `get_yml` после `wf_yml` собрать ссылки из `link_entities` и `schedule_params.entities` yml workflow,
  сверить с `e_ids` (то, что уходит в `ctl_entity.yml`); не найденные — таблица в заметке
  «⚠️ Ссылки на сущности вне выгрузки: N» (workflow, сущность, где) и `logger.warning`.
- [ ] Стенд: ручной запуск `CTL_HR_Data.yml` против заглушки; заметка со ссылками вне поддерева, файлы выгружены.
