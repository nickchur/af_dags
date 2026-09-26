"""Движок srv_wf и отчёты CTL для стенда: GP/*.sql → PostgreSQL.
*2026-09-26 20:25 MSK · v1.1 · Nick Churkin · NSChurkin@sber.ru*

Источник — снимок боевого SQL в `GP/srv_wf/` (его не правим). Сборщик берёт файлы как есть
и механически убирает то, чего в PostgreSQL нет: `DISTRIBUTED …`, параметры хранения
`appendonly`/`orientation`/`compress*`, `EXECUTE ON ANY`. Всё остальное — боевой код: отчёты
на стенде строятся тем же SQL, что в Greenplum, поверх журнала, который наполняют тестовые
прогоны (test_mode) и ответы эмулятора CTL (pr_log_ctl пишет их в tb_log_ctl).

    python3 testbed/gp_engine/build.py > /tmp/engine.sql   # или deploy.sh

Порядок важен: таблицы, базовые функции, вьюхи, движок, вёрстка, отчёты.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / 'GP'
HERE = Path(__file__).resolve().parent

ORDER = [
    'srv_dq/tables/tb_ztest_data', 'srv_dq/tables/tb_ztest_config', 'srv_dq/views/vw_ztest',
    'srv_wf/tables/tb_log_ctl', 'srv_wf/tables/tb_swf_ctl_log', 'srv_wf/tables/tb_swf_mail_log',
    'srv_wf/tables/tb_ctl_alerts', 'srv_wf/tables/tb_log_workflow_stat', 'srv_wf/tables/tb_log_workflow', 'srv_wf/tables/tb_log_workflow_err',
    'srv_wf/functions/try_cast2int', 'srv_wf/functions/is_valid_json', 'srv_wf/functions/try_cast2json',
    'srv_wf/functions/try_cast2jsonb', 'srv_wf/functions/try_cast2timestamp', 'srv_dq/functions/pr_ztest_all_diff', 'srv_wf/functions/pr_log_error', 'srv_wf/functions/pr_log_action',
    'srv_wf/functions/pr_log_stat', 'srv_dq/functions/pr_ztest_set',
    'srv_wf/functions/pr_swf_log_action', 'srv_wf/functions/pr_log_ctl',
    'srv_wf/functions/pr_log_start', 'srv_wf/functions/pr_log_end',
    'srv_wf/views/vw_log_ctl', 'srv_wf/views/vw_log_ctl_loading', 'srv_wf/views/vw_log_ctl_entity',
    'srv_wf/views/vw_log_ctl_wf', 'srv_wf/views/vw_swf_ctl_log',
    'srv_wf/functions/pr_swf_start_ctl',
    'srv_wf/functions/pr_mail_style', 'srv_wf/functions/pr_tbl2html_style', 'srv_wf/functions/pr_tbl2html',
    'srv_wf/functions/pr_tbl2html_loop', 'srv_wf/functions/pr_send_mail',
    'srv_wf/functions/pr_check_etl', 'srv_wf/functions/pr_check_ctl',
    'srv_wf/functions/pr_mail_ctl_status', 'srv_wf/functions/pr_mail_ctl_alerts',
]

# Что убираем. Регистр в боевом SQL разный ('DISTRIBUTED RANDOMLY' в DDL, 'distributed
# randomly' во временных таблицах функций) — все шаблоны без учёта регистра.
_GP_ONLY = [
    (re.compile(r'\s*WITH\s*\([^)]*appendonly[^)]*\)', re.I), ''),
    (re.compile(r'\s*DISTRIBUTED\s+(RANDOMLY|REPLICATED|BY\s*\([^)]*\))', re.I), ''),
    (re.compile(r'\s*EXECUTE\s+ON\s+(ANY|MASTER|ALL\s+SEGMENTS)', re.I), ''),
]


# COMMENT ON вырезаем: описания объектов стенду не нужны, а в части файлов они с дефектом —
# неэкранированные кавычки внутри строки (vw_log_ctl_entity: obj = 'entity'), и файл падает.
# Конец комментария — «';» в конце строки: внутренняя кавычка идёт с другим знаком после.
# Через «;» шаблон не шагает: комментарий без «';» на конце строки не утащит за собой
# следующие операторы, а останется в сборке и упадёт на прогоне — это видно сразу.
_COMMENT = re.compile(r"^\s*comment\s+on\s+[^;]*?';\s*$", re.I | re.M)


def to_pg(sql: str) -> str:
    """Боевой SQL Greenplum → PostgreSQL: только удаление GP-специфики и «OR REPLACE»."""
    sql = _COMMENT.sub('', sql.replace('\r\n', '\n'))
    for rx, repl in _GP_ONLY:
        sql = rx.sub(repl, sql)
    sql = re.sub(r'^CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION', sql, flags=re.M)
    sql = re.sub(r'^CREATE TABLE(?!\s+IF\s+NOT\s+EXISTS)', 'CREATE TABLE IF NOT EXISTS', sql, flags=re.M | re.I)
    sql = re.sub(r'^CREATE (OR REPLACE )?VIEW', 'CREATE OR REPLACE VIEW', sql, flags=re.M)
    return sql.rstrip().rstrip(';') + ';\n'


def build() -> str:
    parts = [(HERE / '05_stubs.sql').read_text()]
    for name in ORDER:
        parts.append(f'\n-- ==== GP/{name}.sql\n')
        parts.append(to_pg((ROOT / f'{name}.sql').read_text()))
    return ''.join(parts)


if __name__ == '__main__':
    sys.stdout.write(build())
