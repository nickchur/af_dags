# Tasks

- [x] `pool_size()` только чтение; `pool_slots()` только число, без динамики; `get_current_load` через `pool_size` — `plugins/utils.py`
- [x] `chk_any_conn(manage_pool=False)`; устаревший диапазон → 20 с предупреждением — `plugins/ctl_core.py`
- [x] Журнал в GP при открытом `gp_pool`, параметрами; честный docstring `rate_limit` — `plugins/ctl_utils.py`
- [x] `test_conn` → `default_pool` + `manage_pool=True`; `set_events`, `ctl_add_end` → `default_pool`; группа `chk_conn` убрана — `ctl_worker/`
- [x] `ctl_config.py`, `ctl_config.json`: `ctl` 20, у `pg`/`ppl`/`s3`/`tfs` без пулов; `ctl_worker/readme.md`
- [x] Сторож не завершается на сбое проверки (иначе `soft_fail` → skipped до конца рана, пул закрыт до часа после оживления) — `ctl_worker/ctl_test_conn.py`; найдено на стенде
- [x] Стенд, 14.09.2026:
  - разбор `ctl_worker` без ошибок, пулы задач по правилу, `pg_pool` ни у одной;
  - после выкладки размер пула писал только `test_conn`; ещё писал `ctl_monitor`, но это
    процесс, стартовавший до выкладки со старым кодом, — прежний код писал `gp_pool` на
    каждый GET к CTL, в одном логе 1312 записей;
  - устаревший `[10, 50]` в Variable → `ctl_pool` 20 и предупреждение;
  - предварительные `chk_any_conn('ctl')` / `('gp')` и `pool_size()` — 0 записей в
    `slot_pool`, несуществующий пул не создан;
  - GET к CTL при открытом `gp_pool` → строки в `tb_log_ctl`;
  - CTL остановлен → сторож обнулил `ctl_pool` за минуту; сторож после сбоя не
    завершается, пул возвращается на следующей проверке;
  - чистый цикл `run_prm → run_exe → run_end` на эмуляторе — success
- [ ] Альфа после выкладки: перезапустить `ctl_config`, удалить `pg_pool`/`ppl_pool`/`s3_pool`/`tfs_pool`
