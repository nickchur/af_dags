#!/usr/bin/env bash
# Движок srv_wf и отчёты CTL на стенд (testsrv): сборка GP/*.sql → PG и прогон в gp_test.
# 2026-09-26 · v1.0 · Nick Churkin · NSChurkin@sber.ru
#
#   bash testbed/gp_engine/deploy.sh            # из корня репозитория
#
# Перед прогоном снимает бэкап схем srv_wf и srv_dq в /opt/aftest/gp_test-srv-backup-*.sql.
# После — пересоздаёт вьюхи стенда обмена (gp_exchange/20_views.sql): замена таблиц-двойников
# вьюхами уносит зависимые вьюхи каскадом.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
python3 "$here/build.py" > /tmp/engine.sql
scp -q /tmp/engine.sql "$here/../gp_exchange/20_views.sql" testsrv:/tmp/
ssh testsrv 'set -e; u=$(docker exec aftest-postgres printenv POSTGRES_USER)
  docker exec aftest-postgres pg_dump -U $u -d gp_test -n s_grnplm_vd_hr_edp_srv_wf -n s_grnplm_vd_hr_edp_srv_dq \
    > /opt/aftest/gp_test-srv-backup-$(date +%Y%m%d-%H%M).sql
  docker exec -i aftest-postgres psql -U $u -d gp_test -v ON_ERROR_STOP=1 -q < /tmp/engine.sql 2>&1 \
    | grep -v "^NOTICE\|already exists, skipping\|^DETAIL\|^drop cascades\|^INFO" || true
  docker exec -i aftest-postgres psql -U $u -d gp_test -q < /tmp/20_views.sql 2>&1 | grep -v "already exists" || true
  rm -f /tmp/engine.sql /tmp/20_views.sql'
echo "движок развёрнут"
