# 🧰 Тестовый стенд
*2026-09-14 06:53 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

То, что нужно, чтобы гонять DAG'и репозитория живьём на стенде (`ssh testsrv`,
`/opt/aftest`): эмуляторы внешних систем, схемы, фикстуры, скрипты разворачивания. Здесь нет
DAG'ов, и на контуры это не нужно.

**Из разбора Airflow каталог убран корневым `.airflowignore`.** Одна точка в имени каталога
здесь не помогла бы: Airflow 2.11 обходит папку дагов через `os.walk` и отбрасывает только
то, что указано в `.airflowignore` (`airflow/utils/file.py`, `_find_path_from_directory`),
скрытые каталоги он не пропускает. Без `.airflowignore` каждый `.py`, где встречаются слова
«airflow» и «dag», импортировался бы на каждом круге dag-processor'а на всех контурах.

| Каталог | Что это |
|---|---|
| [`ctl_worker/`](ctl_worker/README.md) | Эмулятор CTL API (без Kerberos и Greenplum) и сборщик фикстур из снимка `edpetl-ctl`: полный цикл `run_prm → run_exe → run_end` на стенде |
| [`gp_exchange/`](gp_exchange/README.md) | Greenplum на PostgreSQL: пакет обмена собирается по-настоящему и проезжает весь путь до `gp_vw_*` |
| [`vault/`](vault/make_vault.py) | `make_vault.py` — эмуляция `/vault/secrets/application`: payload в формате боевого sigma DEV, секреты только из переменных окружения |

Раньше каталоги лежали внутри своих проектов (`ctl_worker/testbed/`, `gp_exchange/testbed/`),
а `make_vault.py` — в `check/`, среди DAG'ов проверки. Сервисы на стенде от переезда не
зависят: эмулятор CTL запущен из своей копии в `/opt/aftest/ctl-mock`.

## make_vault.py

Собирает файл, который на боевом контуре кладёт vault, а на стенде класть некому. Формат
повторяет боевой payload sigma DEV, чтобы secret backend ходил по тем же веткам.

- 🔑 Секретов в файле нет: логины и пароли берутся из переменных окружения, умолчания —
  заведомо нерабочие заглушки.
- Запуск и набор переменных — в docstring самого скрипта.
