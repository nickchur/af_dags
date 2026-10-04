## 1. Перенос

- [x] 1.1 `git mv` модулей в `ctl_worker/`, импорты `plugins.ctl_*` → `ctl_worker.ctl_*` во всех дагах
- [x] 1.2 `.airflowignore`: `^ctl_worker/ctl_(core|utils)\.py$`
- [x] 1.3 Тексты: readme `plugins/`, `ctl_worker/`, корневой, `GP/`, `tfs_kafka/`, `openspec/project.md`
- [x] 1.4 При архивации: Purpose `specs/plugins` без «обёрток над CTL API», строки
      «Происхождения» перенесённых требований — в `specs/ctl-worker`
- [x] 1.5 Общая `chk_conn` в `plugins/utils.py`; `chk_any_conn` — обёртка с конфигом и пулами;
      копия в `tools/test_connections.py` удалена

## 2. Проверка

- [x] 2.1 ruff без новых замечаний, `testbed/check_status_contract.py` — «ВСЁ РАЗОБРАНО»
- [x] 2.2 Стенд через MCP: ошибок импорта нет, `CTL.*` все на месте, живые раны без `ImportError`
- [x] 2.3 Стенд через MCP: `CTL.HR_Data.test_conn` и `tools_test_connections` проходят на `chk_conn`
