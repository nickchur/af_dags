#!/bin/sh
# Создаёт базу KDC и принципалы при первом старте, выгружает keytab'ы, которых ещё нет.
# 2026-10-02 10:12 MSK · v1.0 · Nick Churkin · NSChurkin@sber.ru
#
# KRB5_PRINCIPALS — «принципал=файл.keytab» через пробел. Keytab выгружается только если
# файла нет: ktadd меняет ключ (kvno), и уже розданные keytab'ы перестали бы подходить.
set -e
REALM="${KRB5_REALM:-STAND.TEST}"
if [ ! -f /var/lib/krb5kdc/principal ]; then
    kdb5_util create -s -r "$REALM" -P "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fi
for item in $KRB5_PRINCIPALS; do
    princ="${item%%=*}"; keytab="/keytabs/${item#*=}"
    kadmin.local -q "getprinc $princ" 2>/dev/null | grep -q "^Principal: $princ" \
        || kadmin.local -q "addprinc -randkey $princ"
    [ -f "$keytab" ] || kadmin.local -q "ktadd -k $keytab $princ"
done
chmod 600 /keytabs/*.keytab 2>/dev/null || true
exec krb5kdc -n
