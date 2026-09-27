CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow AS
 SELECT a.id AS start_id,
    a.ts AS start_ts,
    a.wf_action AS start_action,
    a.wf_message AS workflow,
    b.id AS end_id,
    b.ts AS end_ts,
    b.wf_action AS end_action,
    (b.ts - a.ts) AS duration,
    b.wf_message AS message,
    s.rw_cnt AS rows_count,
    s.data_name AS period_name,
    s.data_min AS period_from,
    s.data_max AS period_to,
    s.load_name,
    s.load_min,
    s.load_max,
    s.key_name,
    s.key_min,
    s.key_max,
    z.zscore,
    ((z.zscore >= COALESCE(((c.z_cfg ->> 'min'::text))::double precision, ('-3.0'::numeric)::double precision)) AND (z.zscore <= COALESCE(((c.z_cfg ->> 'max'::text))::double precision, ((+ 3.0))::double precision))) AS ztest_ok,
    (z.key_date = ANY (c.z_except)) AS is_except,
    COALESCE(((round(((t.p_value * 2.0) * (100)::numeric), 0))::smallint)::integer, 0) AS confidence,
    z.key_date,
    z.key_diff,
    z.value,
    z.avg,
    z.std,
    z.cnt,
    z.min,
    z.max,
    ((z.std)::double precision / (NULLIF(z.avg, 0))::double precision) AS stable
   FROM (((((s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow a
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow b ON ((a.id = b.parent)))
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_stat s ON ((b.id = s.log_id)))
     LEFT JOIN ( SELECT DISTINCT ON (tb_ztest_data.log_id) tb_ztest_data.zscore,
            tb_ztest_data.ts,
            tb_ztest_data.object,
            tb_ztest_data.key_date,
            tb_ztest_data.rows_count,
            tb_ztest_data.key_diff,
            tb_ztest_data.value,
            tb_ztest_data.avg,
            tb_ztest_data.std,
            tb_ztest_data.cnt,
            tb_ztest_data.min,
            tb_ztest_data.max,
            tb_ztest_data.log_id,
            tb_ztest_data.notes
           FROM s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data
          ORDER BY tb_ztest_data.log_id DESC, tb_ztest_data.ts DESC) z ON ((b.id = z.log_id)))
     LEFT JOIN s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_ztable t ON ((round((abs(z.zscore))::numeric, 1) = t.zscore)))
     LEFT JOIN s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config c ON ((z.object = c.object)))
  WHERE (NULLIF(a.parent, 0) IS NULL)
  ORDER BY a.id DESC, b.id DESC;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow is '
Журнал выполнения workflow: начало, завершение, длительность, метрики.
Интегрирован с контролем качества (Z-тест).
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.start_id is 'ID начала';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.start_ts is 'Время старта';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.start_action is 'Действие на старте';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.workflow is 'Параметры workflow';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.end_id is 'ID завершения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.end_ts is 'Время завершения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.end_action is 'Действие на финише';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.duration is 'Длительность';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.message is 'Результат выполнения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.rows_count is 'Число строк';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.period_name is 'Имя периода';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.period_from is 'Начало периода';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.period_to is 'Конец периода';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.load_name is 'Имя загрузки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.load_min is 'Мин. значение';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.load_max is 'Макс. значение';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.key_name is 'Имя ключа';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.key_min is 'Мин. ключ';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.key_max is 'Макс. ключ';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.zscore is 'Z-оценка отклонения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.ztest_ok is 'Результат Z-теста';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.is_except is 'Дата в исключениях';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.confidence is 'Уровень доверия, %';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.key_date is 'Ключевая дата';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.key_diff is 'Разница по ключу';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.value is 'Текущее значение';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.avg is 'Среднее значение';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.std is 'Среднеквадратичное отклонение';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.cnt is 'Число наблюдений';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.min is 'Минимум';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.max is 'Максимум';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow.stable is 'Стабильность (std/avg)';