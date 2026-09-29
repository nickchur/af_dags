# Tasks

## 1. Потолок запроса в Greenplum

- [x] 1.1 `plugins/ctl_core.py`: `gp_timeout(params)` — `wf_timeout` (минуты числом или
      `str2timedelta`), иначе `gp_timeout` из конфига (`minutes=265`; до 24.09.2026 — `minutes=175`), и признак «не ниже
      `gp_server_limit`». Проверка: 15 → 15 мин, `minutes=1` → 1 мин, пусто → 4 ч 25,
      600 → 10 ч с предупреждением.
- [x] 1.2 `ctl_worker.py`: три копии разбора (`run_tfs`, `run_exe` дважды) → `gp_timeout`;
      предупреждение в лог и заметку `Run_prm`. Проверка: заметка показывает потолок и
      предупреждение при 600.
- [x] 1.3 `run_exe`: `execution_timeout` = потолок + 10 мин; остальным — `task_timeout`.
      Проверка на стенде: прерывает ли `execution_timeout` задачу, висящую в `pg_sleep`.
      **Не прерывает** (стенд 29.09.2026): лимит 20 с, `pg_sleep(90)` через `gp_exe` — таск упал
      по таймауту на 90-й секунде, запрос в базе дожил до конца. Обрывает запрос только
      `statement_timeout`; поэтому он ниже `execution_timeout` (лестница), комментарий в `run_exe`.
- [x] 1.4 Снять `sla=sla_time`, ключ `sla_time`; `exe_timeout` из описания дага.

## 2. Монитор

- [x] 2.1 Пороги `lock_stale`, `run_stale`, `new_grace`, `wait_grace` в конфиг, значения по
      умолчанию — нынешние. Проверка: без ключей поведение прежнее.
- [x] 2.2 `RUNNING/RUN` → `reRunned` не раньше max(`run_stale`, `wf_timeout` + 10 мин).
- [x] 2.3 Санитар: ран запаузенного дага в `RUNNING` без задач в `running` старше
      `zombie_after` закрывается, незаконченные задачи — `skipped`, загрузка не трогается.
      Режим «только показать» показывает его отдельно. SQL проверен на PG16 фикстурами:
      8 случаев (пауза в работе/в очереди/свежая/с running-задачей, осиротевший, призрак,
      reschedule-сенсор) — все как в спеке.
- [x] 2.4 Проверка лестницы: `gp_timeout < gp_server_limit`,
      `zombie_after > gp_timeout + 10 мин`, `run_stale ≥ zombie_after`.

## 3. Конфиг и документы

- [x] 3.1 `ctl_config.py`: новые ключи со значениями по умолчанию; `exe_timeout`,
      `sla_time` убраны.
- [x] 3.2 `ctl_worker/readme.md` — лестница и таблица ключей; `GP/readme.md` — откуда
      берётся `statement_timeout`.

## 4. Проверка

- [x] 4.1 `ruff check --select F`, `openspec validate --strict`.
- [x] 4.2 Стенд AF2 с эмулятором CTL — сценарии из спеки (29.09.2026): LOCK 3 ч — не тронута,
      LOCK 6 ч — reStarted, RUNNING/RUN 8 ч при `wf_timeout` 600 — не тронута, 11 ч — reRunned,
      TIME-WAIT идущей загрузки — не тронута; `timeout_ladder` ловит все три нарушения. Попутно
      найден и исправлен вызов `gp_exe(None, sql)` в SLA-ветке монитора: при `wf_interval` у
      загрузки весь прогон монитора молча уходил в skipped (soft_fail).
- [x] 4.3 agy-ревью диффа. Оба «дефекта» опровергнуты: импорт `AirflowFailException` есть
      в обоих файлах; таймаут reschedule-сенсора считается от первой попытки рана
      (`sensors/base.py:260`, `first_try_number = max_tries - retries + 1`), а не от текущей.
      Его «точно прерывает psycopg2» — не принято на веру, решает 1.3 на стенде.
