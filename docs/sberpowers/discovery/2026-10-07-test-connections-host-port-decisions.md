Open questions: 0
# Хосты и порты в tools_test_connections — конспект решений
*2026-10-07 · статус: согласовано*

Запрос: вернуть в `tools_test_connections` хосты и порты подключений, как было в
`show_connections` (удалён в E360-6385), и отдать их в XCom.

Факты:
- `show_connections` писал таблицу `conn_type, conn_id, host, port, schema, description`
  в заметку таска; заметка режется на 1000 символов (`MAX_NOTE_LEN`), таблица на сотню
  подключений обрезалась.
- Сейчас заметка `collect` — строка на тип с именами; host/port только в Variable
  `local_connections`. XCom `collect` — `group, conn_id, conn_type`; XCom `check` —
  `status, conn_id, conn_type`.
- REQ-tools-02: экземпляр check подписан «группой по типу и именем подключения».
- У S3 и Kafka `host`/`port` пусты: адрес в extra (`endpoint_url`, `bootstrap.servers`);
  у show_connections там тоже было `None`.

## Допрос

- **Вопрос:** где показывать host:port?
  **Выбор:** подпись экземпляра `check` в сетке, строки падений в заметке `report`, полная
  таблица (`conn_type, conn_id, host, port, schema, description`) в логе `collect`; заметка
  `collect` остаётся строкой на тип.
  **Почему:** адрес нужен рядом с упавшей проверкой; заметка 1000 символов — таблица в ней
  обрезается.

- **Вопрос:** что в XCom?
  **Выбор:** повторить XCom `show_connections` — словарь `{conn_type: [{conn_id, host, port,
  schema, description}]}`, `sqlite` показан как `clickhouse` (то же, что в Variable
  `local_connections`).
  **Почему:** решение пользователя: как было в show_connections.

- **Вопрос:** под каким ключом XCom словарь, если `return_value` collect занят списком для expand?
  **Выбор:** отдельный ключ `connections` у таска `collect`; `return_value` остаётся списком.
  **Почему:** expand требует список; отдельный таск ради одного XCom — лишнее звено.

- **Вопрос:** добавлять host/port в XCom каждого `check`?
  **Выбор:** нет, `check` возвращает `{status, conn_id, conn_type}` как сейчас.
  **Почему:** адрес есть в подписи, в report и в `collect.connections` по `conn_id`; дубль не нужен.

- **Вопрос:** откуда брать адрес, если `host` пуст (S3, Kafka)?
  **Выбор:** универсальное правило для всех типов:
  1. `host` заполнен — `host` и `port` как есть;
  2. `host` пуст — первый непустой ключ extra из списка `endpoint_url`, `bootstrap.servers`;
  3. в значении нет запятой — разложить на host и port (`scheme://h:p` или `h:p`), без порта — port пуст;
  4. в значении есть запятая — host = хосты без портов через запятую (`h1,h2`), port — от первого
     (у первого порта нет — port пуст).
  Адрес одинаков во всех местах: подпись, report, лог, XCom `connections`, Variable `local_connections`.
  **Почему:** решение пользователя — правило не по типам, а по виду значения; формат как у secret
  backend для PG (`vault_secret_backend.py:41-42`: `h1,h2` + порт первого). Ключ `host` в extra не
  нужен: у HTTP адрес в полях подключения. Разные порты у брокеров покажут только первый — то же
  допущение, что у backend. Variable читает только `test_kafka` и только `conn_id`.

## Open

Нет. Итоговый список подтверждён пользователем 2026-10-07.
