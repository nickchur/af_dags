## 1. Код и тексты

- [x] 1.1 Пульс: `control.reply_bindings`, ⚠️ от 1000 (`tools/system_health.py`)
- [x] 1.2 `tools_queue_analyze`: таск `pidbox`, `max_reply_bindings`, `purge_pidbox`, вывод 📮, строка отчёта
- [x] 1.3 Шапки модулей, `tools/readme.md`, навыки `tools`, `tools-system-health`, `tools-queue-analyze`

## 2. Проверка

- [x] 2.1 ruff без новых замечаний
- [x] 2.2 Стенд через MCP: пустой сет — ☮️; засеянный 1500 записями — ⚠️ пульса и 📮 разбора;
      `purge_pidbox` удаляет сет, следующий пульс чистый, воркеры отвечают на ping
