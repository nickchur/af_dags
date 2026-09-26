-- Подготовка стенда к настоящему движку srv_wf (testbed/gp_engine/build.py).
-- 2026-09-26 · v1.0 · Nick Churkin · NSChurkin@sber.ru
--
-- До движка стенд обмена (testbed/gp_exchange/10_schema.sql) держал вьюхи журнала таблицами
-- с затравкой. Настоящие вьюхи с теми же именами и колонками их заменяют: удаляем таблицы,
-- только если это ещё таблицы — повторный прогон вьюхи не трогает. Зависимые вьюхи обмена
-- (vw_exchange_log_keys) уходят каскадом, deploy.sh пересоздаёт их из gp_exchange/20_views.sql.

CREATE SCHEMA IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf;
CREATE SCHEMA IF NOT EXISTS s_grnplm_vd_hr_edp_srv_dq;

DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind = 'r' AND (
                   (n.nspname = 's_grnplm_vd_hr_edp_srv_wf'
                    AND c.relname IN ('vw_log_ctl_loading', 'vw_log_ctl_entity', 'vw_log_ctl_wf', 'vw_swf_ctl_log'))
                OR (n.nspname = 's_grnplm_vd_hr_edp_srv_dq' AND c.relname = 'vw_ztest'))
    LOOP
        EXECUTE format('DROP TABLE %I.%I CASCADE', r.nspname, r.relname);
        RAISE NOTICE 'таблица-двойник %.% удалена', r.nspname, r.relname;
    END LOOP;
END $$;

-- Журнал ответов CTL заглушки (testbed/ctl_worker/schema.sql) другой структуры: url, msg text
-- обрезанный до 4000. Настоящему pr_log_ctl нужны obj и msg jsonb. Журнал не удаляем —
-- переименовываем, боевая таблица создаётся рядом.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 's_grnplm_vd_hr_edp_srv_wf' AND table_name = 'tb_log_ctl')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 's_grnplm_vd_hr_edp_srv_wf' AND table_name = 'tb_log_ctl' AND column_name = 'obj')
    THEN
        ALTER TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl RENAME TO tb_log_ctl_mock_old;
        RAISE NOTICE 'журнал заглушки переименован в tb_log_ctl_mock_old';
    END IF;
END $$;

-- Последовательности журналов. В HR_Data их DDL нет (в бою заведены отдельно), а таблицы
-- берут id из них по умолчанию.
CREATE SEQUENCE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_swf_ctl_log_id_seq;
CREATE SEQUENCE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_swf_mail_log_id_seq;
CREATE SEQUENCE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_swf_chk_log_id_seq;
CREATE SEQUENCE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_swf_wf_id_seq;
CREATE SEQUENCE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_id_seq;
