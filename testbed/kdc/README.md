# 🎟️ Kerberos на стенде: KDC, CTL и GP по билету

*2026-10-02 10:23 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

На альфе CTL API и Greenplum пускают только по Kerberos: `KerberosHttpHook` (SPNEGO) и libpq
(GSSAPI) берут билет из кэша пода, пароль в подключении GP пустой. Без KDC на стенде эти ветки
кода не выполнялись вовсе. Теперь на стенде AF2 они настоящие.

| Что | Где |
|---|---|
| KDC (MIT krb5), realm `STAND.TEST` | контейнер `aftest-kdc`, `127.0.0.1:88` tcp/udp, [`docker-compose.yml`](docker-compose.yml) |
| Принципалы и keytab'ы | `hrplt_etl` (Airflow), `HTTP/ctl-mock.stand` (эмулятор CTL), `postgres/gp.stand` (GP); keytab'ы — `/opt/aftest/kdc/keytabs`, 600 |
| Клиент хоста | [`krb5.conf`](krb5.conf) → `/etc/krb5.conf`, пакет `krb5-user`; `/etc/hosts`: `kdc.stand ctl-mock.stand gp.stand` → 127.0.0.1 |
| Билет Airflow | юнит [`airflow-kerberos`](airflow-kerberos.service) (`airflow kerberos`); в `airflow.env` — `AIRFLOW__KERBEROS__PRINCIPAL/KEYTAB/CCACHE` и `KRB5CCNAME` |
| CTL | `ctl-mock` с `CTL_MOCK_KERBEROS_KEYTAB` (см. [`ctl_worker`](../ctl_worker/README.md#kerberos)), подключение `ctl` → `ctl-mock.stand`, `extra: {"kerberos_auth": true}` |
| GP | база `adb_dev_comm` (как на альфе); `pg_hba`: `host adb_dev_comm hrplt_etl all gss include_realm=0 krb_realm=STAND.TEST`; keytab — `$PGDATA/krb5.keytab` (`krb_server_keyfile`) |

Подключения — **из секретов, как на контуре**: `ctl` из `HTTP_CONNECTIONS`, GP —
`alpha-adb_dev_comm-read/-write` из `PG_ALPHA` (сервис `cap_gp`), пароль пустой.
Payload собирает [`../vault/make_vault.py`](../vault/make_vault.py).

## Пароль и билет друг другу не мешают

Проверено на стенде — так же, как на контуре:

- **Kerberos-роль с паролем в секретах** входит по билету: сервер просит GSSAPI, libpq пароль
  не использует (`pg_stat_gssapi.gss_authenticated = true`).
- **Парольная роль при билете другой учётки** входит по паролю: libpq (`gssencmode=prefer`)
  шифрует канал GSS, но аутентификация парольная (`gss_authenticated = false`,
  `encrypted = true`).
- **Без билета** Kerberos-роль получает отказ, CTL — `401`.

## Развернуть

```bash
scp Dockerfile entrypoint.sh kdc.conf krb5-kdc.conf krb5.conf docker-compose.yml airflow-kerberos.service testsrv:/opt/aftest/kdc/
ssh testsrv 'cd /opt/aftest/kdc && docker compose up -d --build'
# хост: /etc/krb5.conf, /etc/hosts, apt-get install krb5-user; kinit -kt keytabs/hrplt_etl.keytab hrplt_etl@STAND.TEST
# GP: docker cp keytabs/gp.keytab aftest-postgres:/var/lib/postgresql/data/krb5.keytab (chown postgres, 600),
#     ALTER SYSTEM SET krb_server_keyfile, строка gss в pg_hba.conf, роль hrplt_etl IN ROLE airflow, pg_reload_conf()
```

Новая учётка (GP переходит на Kerberos целиком, учёток станет несколько) — строка
`принципал=файл.keytab` в `KRB5_PRINCIPALS` и перезапуск контейнера.

Keytab выгружается только при первом создании принципала: `ktadd` меняет ключ, и розданные
keytab'ы перестали бы подходить. Пересоздать — удалить файл keytab и перезапустить контейнер.

⚠️ Ключ `krb5.keytab` лежит в каталоге данных Postgres, а не в образе: пересоздание
контейнера `aftest-postgres` его не теряет, но новый том — теряет.
