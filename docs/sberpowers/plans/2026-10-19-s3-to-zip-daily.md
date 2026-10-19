# План: `tools_s3_to_zip` — ежедневно, массив src, add_ts, сохранение параметров

> Исполнитель — я, ревью — DeepSeek v4-pro + agy flash-high в фоне. Шаги — чекбоксы.

**Спека:** docs/sberpowers/specs/2026-10-19-s3-to-zip-daily.md (MODIFIED 01, 03, 04, 05, 10, 11; ADDED 12)
**Цель:** даг по расписанию раз в сутки архивирует вчерашнее (MSK) по каждому src в свой ZIP; настройки — в переменной.
**Архитектура:** `s3_to_zip` остаётся «один src → один архив»: даты по MSK, пустой отбор → skip, имя/метка приходят
аргументами (`name`, `ts`), отмена хвостов — по имени. Даг: `params` (сохранение) → `jobs` (чистая функция `_jobs`:
вчера по MSK, имена с номерами, одна метка) → `archive.expand(job=…)` по одному экземпляру за раз.

## Глобальные ограничения

- Worktree `~/wt/s3-to-zip-daily`, ветка `feat/s3-to-zip-daily` от `af/develop_3` (756db73).
- AF3 (`airflow.sdk`), Python 3.10; `plugins/s3_utils.py` в CRLF — правка через Python с `newline=''`, `git ls-files --eol`.
- Строка версии — `TZ=Europe/Moscow date` перед каждой меткой; s3_utils v1.8 → v1.9, даг v1.0 → v1.1 (мажор не ставлю
  сам — предложу), readme v1.4 → v1.5.
- Сохранение параметров — механика `log_cleanup`: `PARAMS_VAR`, `saved_params`, `_param`, `saved_schedule`,
  `store_params_task(..., one_shot=…)`, потомок `params` — `TriggerRule.NONE_FAILED`.
- Тесты — `python -m pytest tests -q` в `/opt/aftest/venv312` (копия на стенде `/opt/aftest/wt_s3zip`). `ruff check` до коммита.

## Волны

- Волна 1: задача 1.1
- Волна 2: задача 2.1 (blocked by 1.1)

### Задача 1.1: `s3_to_zip` — MSK, skip, name/ts, отмена по имени (волна 1)

**REQ:** 03, 04, 05, 11
**Blocked by:** нет

**Интерфейс (производит):**
`s3_to_zip(src_path, dst_path, date_from=None, date_to=None, max_bytes=0, replace=False, part_size=64 * 1024**2,
name=None, ts=None) -> dict` — `name` по умолчанию последняя папка src (или бакет); `ts` — строка метки или None;
пустой отбор — `AirflowSkipException`.

**Шаги:**
- [ ] Тесты в `tests/test_s3_to_zip.py` (правка существующих + новые):

```python
def test_mask_dates_and_name_ts(cl):
    r = s3_utils.s3_to_zip('s3://b/logs/*/*.log', 's3://b/arc/', date_from='2026-10-05', date_to='2026-10-05',
                           ts='20261019_013000')
    assert r['path'] == 's3://b/arc/logs_20261019_013000.zip'
    assert unzip(cl, 'arc/logs_20261019_013000.zip') == {'b/3.log': b'y' * 10}
    r = s3_utils.s3_to_zip('s3://b/logs/b/', 's3://b/arc/x.zip', name='ignored', ts='T')
    assert r['path'] == 's3://b/arc/x_T.zip'                      # метка и в ключе .zip
    r = s3_utils.s3_to_zip('s3://b/logs/b/', 's3://b/arc/', name='logs_2')
    assert r['path'] == 's3://b/arc/logs_2.zip'                   # без метки — имя как есть


def test_dates_msk_boundary(cl):
    """23:30 UTC 4 октября — это уже 5 октября по MSK."""
    cl.objs['msk/late.log'] = (b'L', datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc))
    cl.objs['msk/early.log'] = (b'E', datetime(2026, 10, 4, 20, 30, tzinfo=timezone.utc))
    r = s3_utils.s3_to_zip('s3://b/msk/', 's3://b/arc/m.zip', date_from='2026-10-05', date_to='2026-10-05')
    assert unzip(cl, 'arc/m.zip') == {'late.log': b'L'} and r['count'] == 1


def test_empty_skips(cl):
    with pytest.raises(AirflowSkipException, match='нечего'):
        s3_utils.s3_to_zip('s3://b/nope/', 's3://b/arc/')
    assert 'arc/nope.zip' not in cl.objs and not cl.parts


def test_abort_stale_uploads_own_name_only(cl):
    cl.parts = {'arc/logs_20261018_013000.zip': [b'p'], 'arc/logs.zip': [b'p'], 'arc/logs_2_20261018_013000.zip': [b'p'],
                'arc/other_20261018_013000.zip': [b'p'], 'arc/sub/logs.zip': [b'p']}
    r = s3_utils.s3_to_zip('s3://b/logs/', 's3://b/arc/', ts='20261019_013000')
    assert sorted(r['aborted']) == ['arc/logs.zip', 'arc/logs_20261018_013000.zip']
    assert set(cl.parts) == {'arc/logs_2_20261018_013000.zip', 'arc/other_20261018_013000.zip', 'arc/sub/logs.zip'}
```

  Из `test_empty_limit_exists_bad_date` убрать проверку «нечего» (теперь skip); старые `test_mask_dates_and_auto_name`
  и `test_abort_stale_uploads` заменить новыми. Импорт `AirflowSkipException` из `airflow.sdk.exceptions`.
- [ ] Прогон на стенде — FAIL (нет `ts`, нет skip).
- [ ] Реализация в `s3_to_zip`:

```python
MSK = timezone(timedelta(hours=3))   # модульная константа: MSK круглый год UTC+3

# имя и ключ архива
dst_key = dst['key']
if dst_key.lower().endswith('.zip'):
    folder, _, stem = dst_key.rpartition('/')
    folder, stem = folder + '/' if folder else '', stem[:-4]
else:
    folder = dst_key.rstrip('/') + '/' if dst_key else ''
    stem = name or base.rstrip('/').rsplit('/', 1)[-1] or src['bucket']
dst_key = f"{folder}{stem}{'_' + ts if ts else ''}.zip"

# отмена хвостов — только своё имя: <stem>.zip или <stem>_<YYYYMMDD_HHMMSS>.zip
own = re.compile(re.escape(stem) + r'(_\d{8}_\d{6})?\.zip')
... if own.fullmatch(u['Key'][len(folder):]) ...

# даты по MSK
day = o['LastModified'].astimezone(MSK).date()

# пустой отбор
if not objs:
    raise AirflowSkipException(f"Под {src_path} нечего архивировать (даты MSK {date_from}…{date_to})")
```

  `import re`, `timedelta` в импорт datetime, `AirflowSkipException` рядом с `AirflowFailException`; docstring — про
  MSK, name/ts, skip. Тест-метки вида `T` в `x_T.zip` regex отмены не матчит — это ок (метка дага всегда цифровая).
- [ ] Прогон — PASS; `ruff check`; commit `feat(s3-zip): даты по MSK, пустой отбор — skip, имя и метка аргументами (REQ 03, 04, 05, 11)`.

### Задача 2.1: даг — расписание, массив src, add_ts, params (волна 2)

**REQ:** 01, 03, 05, 10, 12
**Blocked by:** 1.1

**Файлы:** `s3_tools/s3_to_zip.py` (v1.1), `s3_tools/readme.md` (v1.5), тест `tests/test_s3_to_zip_jobs.py`.

**Шаги:**
- [ ] Тест чистой функции `_jobs` (через ast, как `tests/test_conn_facts.py` — модуль дага при импорте читает Variable):

```python
"""Раскладка запуска `tools_s3_to_zip` по архивам: вчера по MSK, имена с номерами, одна метка.

Прогон: `python -m pytest tests -q` в окружении Airflow 3 (`/opt/aftest/venv312`).
"""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException

from plugins.s3_utils import s3_path_parse

_SRC = Path(__file__).resolve().parents[1] / "s3_tools" / "s3_to_zip.py"
_tree = ast.parse(_SRC.read_text(encoding="utf-8"))
_ns = {'s3_path_parse': s3_path_parse, 'AirflowFailException': AirflowFailException,
       'datetime': datetime, 'timedelta': timedelta, 'timezone': timezone, 'MSK': timezone(timedelta(hours=3))}
exec(compile(ast.Module([n for n in _tree.body if isinstance(n, ast.FunctionDef) and n.name == '_jobs'], []),
             str(_SRC), "exec"), _ns)
_jobs = _ns['_jobs']

P = {'src': ['s3://b/airflow-logs/', 's3://c/x/airflow-logs/', 's3://b/ctl/'], 'dst': 's3://b/archive/',
     'date_from': None, 'date_to': None, 'add_ts': True, 'max_size_gb': 1, 'replace': False}
NOW = datetime(2026, 10, 18, 22, 30, tzinfo=timezone.utc)   # 19.10 01:30 MSK


def test_names_ts_and_yesterday_msk():
    j = _jobs(P, NOW)
    assert [x['name'] for x in j] == ['airflow-logs', 'airflow-logs_2', 'ctl']
    assert {x['ts'] for x in j} == {'20261018_223000'}
    assert {(x['date_from'], x['date_to']) for x in j} == {('2026-10-18', '2026-10-18')}   # вчера по MSK
    assert j[0]['max_bytes'] == 1024**3 and j[0]['dst'] == 's3://b/archive/'


def test_explicit_dates_no_ts_and_dst_zip():
    j = _jobs({**P, 'src': ['s3://b/a/'], 'date_from': '2026-10-01', 'add_ts': False, 'dst': 's3://b/a.zip'}, NOW)
    assert (j[0]['date_from'], j[0]['date_to'], j[0]['ts']) == ('2026-10-01', None, None)
    with pytest.raises(AirflowFailException, match='папкой'):
        _jobs({**P, 'dst': 's3://b/a.zip'}, NOW)
    with pytest.raises(AirflowFailException, match='src'):
        _jobs({**P, 'src': ['', ' ']}, NOW)
```

- [ ] Даг `s3_tools/s3_to_zip.py`:

```python
PARAMS_VAR = 'tools_s3_to_zip_params'
DEFAULT_SCHEDULE = '30 1 * * *'          # 01:30 MSK: вчерашние сутки MSK закрыты
MSK = timezone(timedelta(hours=3))
_LOG_PREFIX = ...                       # как было
DEFAULT_SRC = log_bucket_path(f'{_LOG_PREFIX}/' if _LOG_PREFIX else '')
ensure_pool(TOOLS_POOL)
SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


def _jobs(p, now):
    """Раскладка запуска по архивам: на каждый src — свой; вчера по MSK, имена с номерами, одна метка на ран."""
    srcs = [s.strip() for s in p['src'] if s and s.strip()]
    if not srcs:
        raise AirflowFailException('src пуст: нужен хотя бы один путь conn_id://бакет/путь')
    if len(srcs) > 1 and p['dst'].strip().lower().endswith('.zip'):
        raise AirflowFailException(f"src {len(srcs)}, а dst — один ключ .zip: при нескольких src dst должен быть папкой")
    d_from, d_to = p.get('date_from') or None, p.get('date_to') or None
    if not d_from and not d_to:
        d_from = d_to = (now.astimezone(MSK) - timedelta(days=1)).date().isoformat()
    ts = now.astimezone(timezone.utc).strftime('%Y%m%d_%H%M%S') if p.get('add_ts', True) else None
    jobs, seen = [], {}
    for s in srcs:
        sp = s3_path_parse(s)
        base = sp['prefix'] if sp['is_mask'] else sp['key']
        stem = base.rstrip('/').rsplit('/', 1)[-1] or sp['bucket']
        seen[stem] = seen.get(stem, 0) + 1
        jobs.append({'src': s, 'dst': p['dst'].strip(), 'name': stem if seen[stem] == 1 else f'{stem}_{seen[stem]}',
                     'ts': ts, 'date_from': d_from, 'date_to': d_to,
                     'max_bytes': int(float(p['max_size_gb']) * 1024**3), 'replace': bool(p['replace'])})
    return jobs
```

  Params: `src` — `_param('src', [DEFAULT_SRC] if DEFAULT_SRC else [], type='array', items={'type': 'string'}, minItems=1)`;
  `dst`, `add_ts` (True), `max_size_gb` (20), `replace` (False), `schedule` (DEFAULT_SCHEDULE) — через `_param`;
  `date_from`/`date_to` — `Param(None, type=['null','string'], format='date')` из кода (разовые); `save_params` — `Param(False)`.
  DAG: `schedule=saved_schedule(SAVED, DEFAULT_SCHEDULE, PARAMS_VAR)`, `start_date=datetime(2026, 10, 19, tzinfo=MSK)`,
  `catchup=False`, `max_active_runs=1`, `max_active_tasks=1`, `is_paused_upon_creation=True`, `on_failure_callback=on_callback`;
  default_args: `pool=TOOLS_POOL`, `retries=1`, `retry_delay=1 мин`, `priority_weight=900`, `weight_rule='absolute'`,
  `execution_timeout=6 ч`, `on_failure_callback=on_callback`. Таски:

```python
    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию (даты — разовые)."""
        return store_params_task(PARAMS_VAR, SAVED, context, one_shot=('date_from', 'date_to'))

    @task(task_id='jobs', trigger_rule=TriggerRule.NONE_FAILED)
    def jobs(params=None, **context):
        """🧮 Раскладка по архивам: вчера по MSK, имена, одна метка на ран."""
        j = _jobs(params, datetime.now(timezone.utc))
        add_note({x['name']: x['src'] for x in j}, context)
        return j

    @task(map_index_template="{{ job['name'] }}")
    def archive(job, **context):
        """🗜️ Один src → один ZIP: обход, лимит, потоковая запись; итог — в заметке."""
        r = s3_to_zip(job['src'], job['dst'], job['date_from'], job['date_to'], job['max_bytes'], job['replace'],
                      name=job['name'], ts=job['ts'])
        add_note({**r, 'raw': readable_size(r['raw']), 'size': readable_size(r['size'])}, context)
        return r

    planned = jobs()
    save_params() >> planned          # params → jobs (NONE_FAILED) → archive
    archive.override(doc_md=archive.function.__doc__).expand(job=planned)
```
- [ ] readme: таблица параметров (src массив, add_ts, schedule, save_params, даты MSK/вчера), «по архиву на src»,
  имена с номерами, skip пустого, переменная `tools_s3_to_zip_params`; docstring дага — расписание и таски
  `params → jobs → archive`.
- [ ] `ruff check`, тесты на стенде PASS; DagBag на стенде: params/description, schedule `30 1 * * *`.
- [ ] Commit `feat(s3-zip): ежедневный запуск, массив src, add_ts, сохранение параметров (REQ 01, 05, 10, 12)`.

## После волн

Ревью DeepSeek + agy в фоне → свод `docs/sberpowers/reviews/2026-10-19-s3-to-zip-daily.md` → стенд по критериям
приёмки → push, PR в `develop_3`.
