# 🪣 S3-инструменты — служебные DAG'и альфы
*2026-09-24 11:16 MSK · v1.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Инструменты для администрирования и отладки. Все DAG'и запускаются вручную (`schedule=None`).

---

## Файлы

| DAG | ID | Описание |
|---|---|---|
| `s3_checker.py` | `tools_s3_check_logs` | Находит файлы по маске, сортирует, читает содержимое (txt, gz, zip) |
| `s3_set_ttl.py` | `tools_s3_set_ttl` | Просматривает, устанавливает или удаляет TTL-правила S3-бакета |
| `s3_bucket_list.py` | `tools_s3_bucket_list` | Перечисляет все бакеты по всем S3-подключениям с размером и TTL |
| `s3_bucket_viewer.py` | `tools_s3_bucket_viewer` | Просматривает список бакетов через `HrpS3BucketViewerOperator` |
| `s3_viewer.py` | `tools_s3_viewer` | Выводит список ключей и читает содержимое файлов через `HrpS3*Operator` |
| `s3_from_content.py` | `tools_s3_from_content` | Загружает текстовый контент в S3 из параметров запуска |
| `s3_to_s3.py` | `tools_s3_to_s3` | Копирует один объект между S3-бакетами с опциональным сжатием |
| `s3_to_s3_test.py` | `tools_s3_to_s3_test` | Находит файлы по маске и копирует/перемещает их S3→S3 |
| `test_package.py` | `tools_test_package` | Собирает тестовый ZIP-пакет формата ЕР/ТФС (данные, `.meta`, `.tkt`) и кладёт в S3; на PROM не регистрируется |

> Каталог назывался `tools/`; 24.09.2026 переименован в `s3_tools/`, а `tools/` стал
> каталогом служебных дагов (раньше `check/`) — как на сигме. Туда же ушёл `dummy.py`.

---

## dummy — пример Markdown
