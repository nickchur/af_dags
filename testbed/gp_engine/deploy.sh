#!/usr/bin/env bash
# Движок srv_wf и отчёты CTL на стенд (testsrv): сборка GP/*.sql → PG и прогон в adb_dev_comm.
# 2026-10-02 10:22 MSK · v1.1 · Nick Churkin · NSChurkin@sber.ru
#
#   bash testbed/gp_engine/deploy.sh            # из корня репозитория
#
# Перед прогоном снимает бэкап схем srv_wf и srv_dq в /opt/aftest/adb_dev_comm-srv-backup-*.sql.
# После — пересоздаёт вьюхи стенда обмена (gp_exchange/20_views.sql): замена таблиц-двойников
# вьюхами уносит зависимые вьюхи каскадом.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
python3 "$here/build.py" > /tmp/engine.sql
python3 "$here/build.py" --post > /tmp/engine_post.sql
scp -q /tmp/engine.sql /tmp/engine_post.sql "$here/../gp_exchange/20_views.sql" testsrv:/tmp/
ssh testsrv 'set -e; u=$(docker exec aftest-postgres printenv POSTGRES_USER)
  docker exec aftest-postgres pg_dump -U $u -d adb_dev_comm -n s_grnplm_vd_hr_edp_srv_wf -n s_grnplm_vd_hr_edp_srv_dq \
    > /opt/aftest/adb_dev_comm-srv-backup-$(date +%Y%m%d-%H%M).sql
  docker exec -i aftest-postgres psql -U $u -d adb_dev_comm -v ON_ERROR_STOP=1 -q < /tmp/engine.sql > /tmp/engine.log 2>&1 \
    || { grep -v "^NOTICE\|already exists, skipping\|^DETAIL\|^drop cascades\|^INFO" /tmp/engine.log; exit 1; }
  docker exec -i aftest-postgres psql -U $u -d adb_dev_comm -q < /tmp/20_views.sql 2>&1 | grep -v "already exists" || true
  docker exec -i aftest-postgres psql -U $u -d adb_dev_comm -v ON_ERROR_STOP=1 -q < /tmp/engine_post.sql > /tmp/engine.log 2>&1 \
    || { grep -v "^NOTICE\|^INFO" /tmp/engine.log; exit 1; }
  rm -f /tmp/engine.sql /tmp/engine_post.sql /tmp/20_views.sql'
echo "движок развёрнут"
