# Ревью: поддерево сущностей без полного реестра CTL (2026-10-21)

Ревьюеры: DeepSeek v4-pro, agy gemini-3.8-flash-high (Pro в agy нет). Находки сверены с кодом вручную.

| Находка | Кто | Вердикт | Что сделано |
|---|---|---|---|
| При сбое CTL по одной сущности прежнее имя получали и сущности с 4xx | оба | подтверждено | прежнее имя — только сущностям со сбоем |
| `name: null` → `AttributeError` | agy | подтверждено (было и в базе) | `(name or '_Not_found_')` |
| `parentId: null` → `TypeError` в `eids_parents`/`events_parents` | оба | подтверждено (было и в базе) | `or 0`, ноль отсекается |
| 401/403 считались «сущности нет» → имена `_Not_found_`, сломанные Asset | DeepSeek | подтверждено | «нет» — только 404, прочее — сбой |
| Прочие 4xx уходят в `except Exception` | agy | ложная | `ctl_api` превращает любой 4xx в Skip |
| Форма ответа `/v4/api/entity/{id}` не проверена | DeepSeek | снято | контур 21.10: плоский dict `id/name/parentId` |
| Пустой `ctl_entities` на первом запуске урезает `ctl_enames` | agy | не регрессия | так же было в базе; теперь сущности событий спрашиваются адресно |
| Нет тестов на сборку `ctl_enames`/висячих в `load_events` | оба | принято частично | логика внутри задачи; проверено на стенде: переменные ветки и базы совпали |
| generic `Exception` вместо `AirflowFailException` | agy | не принято | Exception даёт повтор задачи — так и нужно при пустом ответе |

Найдено при прогоне: заметка «Ошибка API» на каждую висячую сущность каждые 5 минут — для адресных запросов
выключена (`ctl_api(note=False)`).

## Стенд AF3 (ctl-mock3)

- База `develop_3` и ветка подряд: `ctl_enames`, `ctl_entities`, `ctl_events`, `ctl_entity_events` совпали побайтно.
- `load_entities`: «🌳 поддерево 941010000: 680»; `load_events`: «адресных запросов сущностей: 12, CTL не ответил: []».
- Журнал заглушки после выкладки: только `child?direct=false` и `/v4/api/entity/<id>`, голого `/v4/api/entity` нет.
- `CTL_HR_Data.yml` (REQ-ctl-worker-63): `safe=false` — заметка «⚠️ Ссылки на сущности вне выгрузки: 1»
  (`pc1080.wfg_ref_component`, 131110048, schedule_params); `safe=true` — заметки нет (чужие события в параметре).
- Тесты: `tests/test_ctl_entities.py` + `tests/test_ctl_api_gp_pool.py` — 20 passed.
