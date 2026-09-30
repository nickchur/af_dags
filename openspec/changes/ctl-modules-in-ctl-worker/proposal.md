## Why

`ctl_core` и `ctl_utils` лежат в общей папке `plugins/`, хотя кодом их импортируют только даги
`ctl_worker/` (проверено по репозиторию 30.09.2026). Общий каталог обещает совместимость «для
всех», которой здесь некому требовать, а CTL-код и его требования разнесены по двум местам.

## What Changes

- Модули `plugins/ctl_core.py`, `plugins/ctl_utils.py` переезжают в `ctl_worker/`; импорт —
  `from ctl_worker.ctl_utils import …` от корня папки дагов, одним путём везде.
- `.airflowignore` прячет оба модуля от разбора как DAG-файлов.
- Требования к ним без изменения текста переезжают из `specs/plugins` в `specs/ctl-worker`.
- Двойной импорт (`CI06932748.tools…`) у кода CTL не появляется и не был: он нужен модулям,
  которые едут на сигму, и остаётся у них.

## Capabilities

### New Capabilities

### Modified Capabilities

- `ctl-worker`: принимает требования модулей CTL (API, GP, снимки, повторы, события, дельта).
- `plugins`: отдаёт их; требование о ленивом импорте остаётся за общими модулями.

## Impact

- Код: `ctl_worker/*.py` (импорты), `ctl_worker/ctl_core.py`, `ctl_worker/ctl_utils.py`,
  `.airflowignore`; тексты — readme каталогов, `openspec/project.md`, `GP/readme.md`.
- Выкладка: `plugins/` и `ctl_worker/` — одним заходом; иначе на альфе окажутся даги без модулей.
