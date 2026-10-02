# 📚 Эмулятор Confluence, Jira и Bitbucket
*2026-10-02 13:51 MSK · v1.3 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Корпоративный Confluence у Сбера не один: `confluence.delta.sbrf.ru`, `confluence.sberbank.ru`.
GigaCode ходит в них и в Jira через MCP, который DE подключают сами, контур его не даёт.
MCP бывает двух видов: корпоративный MCP Confluence и MCP Jira (SberWorks, центральный, вход по
личному PAT) и [`mcp-atlassian`](https://pypi.org/project/mcp-atlassian/), по экземпляру на
сервер. Имена инструментов у них разные, навыки знают оба. Чтобы навыки, которые ссылаются на
Confluence, проверялись до контура, на стенде стоит сервер с тем же REST. Проверяется он через
`mcp-atlassian`: корпоративный сервер к стенду не подключить.

| Экземпляр | Что изображает | Порт на стенде |
|---|---|---|
| `atlassian-mock@delta` | `confluence.delta.sbrf.ru` | `127.0.0.1:8090` |
| `atlassian-mock@jira` | Jira | `127.0.0.1:8091` |
| `atlassian-mock@sber` | `confluence.sberbank.ru` | `127.0.0.1:8092` |
| `atlassian-mock@stash-delta` | Bitbucket `stash.delta.sbrf.ru` | `127.0.0.1:8093` |
| `atlassian-mock@stash-sigma` | Bitbucket `stash.sigma.sbrf.ru` | `127.0.0.1:8094` |

Один код — [`atlassian_mock.py`](atlassian_mock.py), сервер выбирает `ATLASSIAN_MOCK_SERVER`.
Только чтение, вход по Bearer PAT, как на Server/DC.

## Что умеет

Набор REST снят прогоном **всех инструментов чтения `mcp-atlassian` 0.23.1** против
эмулятора, а не придуман: на что клиент получал 404, то и добавлено. Неизвестный путь даёт
404 в JSON и строку WARNING в журнале юнита. Так новая версия клиента сама показывает, чего
ей не хватает.

- **Confluence:** страница по id, по пространству и названию; поиск CQL (`/rest/api/search` и
  `/rest/api/content/search`); потомки и дерево пространства; метки; пространства; текущий
  пользователь. Комментарии, вложения и история пустые.
- **Jira:** задача, поиск JQL (GET и POST), комментарии, проекты, поля, пользователь.
  Переходы и типы связей пустые.
- **Веб-ссылки** — `/pages/viewpage.action?pageId=…` и `/display/<SPACE>/<Title>` (`+` —
  пробел) — отдают HTML. Ссылку из навыка можно открыть на стенде, заменив хост на адрес
  эмулятора.

⚠️ CQL и JQL — подмножество: условия через `AND`/`OR` без скобок; поля `text`, `title`,
`space`, `label`, `ancestor`, `parent`, `id`, `type` и `project`, `key`, `summary`,
`description`, `status`, `labels`, `issuetype`; `ORDER BY` — по дате и названию. Поиск по
тексту — все слова запроса в названии или теле, без морфологии. Неизвестное поле
пропускается с предупреждением в журнале.

## Наполнение

```
fixtures/
  confluence/<server>/<SPACE>/<id>.md      шапка YAML (title, parent, labels) + Markdown
  confluence/<server>/<SPACE>/<id>.html    та же шапка + готовый XHTML (импорт HTML-экспорта)
  confluence/<server>/<SPACE>/_space.yaml  name, description (необязательно)
  jira/issues.json                         key, summary, description, status, labels, comments, created, updated
```

- **Корпоративные страницы** — с теми же id, ключами пространств и названиями, что в
  корпоративном Confluence. Тогда настоящая ссылка из навыка находит страницу и здесь.
  Например, `delta`/`HRData`: «HR Data» `1774392110`, описания таблиц GP
  (`ue_circle_of_contacts`). Их содержимое корпоративное и **живёт только на стенде**, в git
  его нет: репозиторий публичный.
- **HTML-экспорт пространства** (штатная выгрузка Confluence: `index.html`, `toc.html`, по
  файлу на страницу) раскладывает [`import_html_export.py`](import_html_export.py) — id и
  названия из заголовков страниц, дерево из `toc.html`, корень — куда ведёт `index.html`.
  Картинок в экспорте нет, вместо них подпись `[изображение]`. Так на стенде лежат
  `sber`/`HRTECH` (документация сервиса авторизации, корень `1247712729`) и `delta`/`HRData`
  (ветка «ПКАП1080» с описаниями таблиц GP, около 1550 страниц, `--parent 1774392110`).

  ```bash
  python3 import_html_export.py <каталог экспорта> --server sber --space HRTECH \
      --out /opt/aftest/atlassian-mock/fixtures
  ```
- **Сгенерированное** собирает [`build_fixtures.py`](build_fixtures.py):
  - пространства `ETL` (docs и навыки etl-core) и `AFDAGS` (readme и навыки af_dags) на
    `delta`;
  - задачи Jira по ключам `HRPDATALAB-*` и `E360-*` из истории коммитов;
  - заглушки страниц, на которые ссылаются код и навыки, если их ещё нет.

  Положенное руками сборщик не трогает. После смены наполнения перезапустите экземпляры:
  страницы читаются один раз при старте.

## Стенд

```bash
python3 build_fixtures.py --etl-core ~/etl-core --af-dags ~/ctl --out /tmp/atl-fixtures
scp -r atlassian_mock.py /tmp/atl-fixtures testsrv:/opt/aftest/atlassian-mock/   # fixtures/ — в одноимённый каталог
# /opt/aftest/atlassian-mock/common.env (chmod 600):
#   ATLASSIAN_MOCK_FIXTURES=/opt/aftest/atlassian-mock/fixtures
#   ATLASSIAN_MOCK_TOKENS=<PAT>
# delta.env: ATLASSIAN_MOCK_SERVER=delta PORT=8090; jira.env: …=jira PORT=8091; sber.env: …=sber PORT=8092
# stash-delta.env: …=stash-delta PORT=8093; stash-sigma.env: …=stash-sigma PORT=8094
systemctl enable --now atlassian-mock@delta atlassian-mock@jira atlassian-mock@sber \
    atlassian-mock@stash-delta atlassian-mock@stash-sigma
```

## Подключение mcp-atlassian

Как у DE: по экземпляру на сервер, Jira — вместе с `delta`. Порты стенда пробрасываются
туннелем (`ssh -L 18090:127.0.0.1:8090 -L 18091:127.0.0.1:8091 -L 18092:127.0.0.1:8092 testsrv`):

```bash
READ_ONLY_MODE=true CONFLUENCE_URL=http://localhost:18090 CONFLUENCE_PERSONAL_TOKEN=<PAT> \
JIRA_URL=http://localhost:18091 JIRA_PERSONAL_TOKEN=<PAT> mcp-atlassian          # confluence-delta
READ_ONLY_MODE=true CONFLUENCE_URL=http://localhost:18092 CONFLUENCE_PERSONAL_TOKEN=<PAT> \
mcp-atlassian                                                                     # confluence-sber
```

## Bitbucket

DDL таблиц и код функций лежат в корпоративном Bitbucket Server, и серверов у него тоже
несколько. Корпоративный «MCP сервер BitBucket CI» пока открыт только агентам AI Hub SberWorks. Агент DE читает их через MCP
[`@atlassian-dc-mcp/bitbucket`](https://www.npmjs.com/package/@atlassian-dc-mcp/bitbucket),
по экземпляру на сервер. Так при разборе падения он сверяет текст ошибки с DDL по ссылке из
навыка. Эмулятор отвечает на REST его инструментов чтения. Набор снят прогоном версии 0.35:

- проекты и репозитории;
- файл по пути и ветке (`/raw`); на каталог — листинг дерева, как у Bitbucket;
- ветки;
- коммиты по пути: кто и когда менял DDL;
- поиск кода (`/rest/search/latest`): все слова запроса в файле, модификаторы `repo:`,
  `project:`, `ext:`;
- пользователь;
- пустые списки PR.

Сравнение веток (`compare/diff`) не эмулируется: для разбора падений оно не нужно.

**Веб-ссылка** `/projects/<P>/repos/<r>/browse/<path>` отдаёт текст файла или листинг.
Ссылку из навыка можно открыть на стенде, заменив хост.

**Наполнение** — bare-клоны `fixtures/bitbucket/<server>/<PROJECT>/<slug>.git`, с теми же
ключами проектов и slug, что на настоящих серверах. Читаются `git`, сервер git не нужен.
Положенный клон виден сразу, перезапуск не нужен. На стенде:

| Сервер | Репозиторий | Что в нём |
|---|---|---|
| `stash-delta` | `BIGDATA/hr_data` | клон HR_Data: DDL GP, `sql/create/<схема>/tables\|functions` |
| `stash-sigma` | `HRPLATFORM/app-dataplatform-etl-dags` | клон af_dags |

```bash
git init --bare hr_data.git && git -C hr_data.git fetch ~/HR_Data '+refs/remotes/origin/*:refs/heads/*'
git -C hr_data.git symbolic-ref HEAD refs/heads/develop     # ветка по умолчанию — как на сервере
# → /opt/aftest/atlassian-mock/fixtures/bitbucket/stash-delta/BIGDATA/hr_data.git
```

Подключение MCP (туннель `-L 18093:127.0.0.1:8093 -L 18094:127.0.0.1:8094`):

```bash
BITBUCKET_API_BASE_PATH=http://localhost:18093/rest BITBUCKET_API_TOKEN=<PAT> \
npx -y @atlassian-dc-mcp/bitbucket                                               # bitbucket-delta
```

Самопроверка разбора CQL, JQL и запросов поиска кода: `python atlassian_mock.py`.
