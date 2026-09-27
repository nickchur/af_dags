CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log AS
 SELECT a.id AS beg_id,
    a.ts AS beg_ts,
    a.wf_action AS beg_action,
    (a.wf_message ->> 'obj'::text) AS obj,
    (a.wf_message ->> 'sch'::text) AS sch,
    (a.wf_message ->> 'key'::text) AS key,
    b.id AS end_id,
    b.ts AS end_ts,
    (b.ts - a.ts) AS duration,
    b.wf_action AS end_action,
    ((b.wf_message ->> 'res'::text))::integer AS res,
    (b.wf_message ->> 'msg'::text) AS msg,
    ((b.wf_message ->> 'last'::text))::timestamp without time zone AS last,
    ((b.wf_message ->> 'value'::text))::json AS value,
    a.wf_message AS beg_message,
    b.wf_message AS end_message
   FROM (s_grnplm_vd_hr_edp_srv_wf.tb_swf_chk_log a
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.tb_swf_chk_log b ON ((b.parent = a.id)))
  WHERE (true AND (a.parent IS NULL));

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log is '
Журнал проверок в рамках SWF: начало, завершение, результат, длительность.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.beg_id is 'ID начала';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.beg_ts is 'Время старта';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.beg_action is 'Действие на старте';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.obj is 'Объект проверки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.sch is 'Расписание';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.key is 'Ключ проверки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.end_id is 'ID завершения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.end_ts is 'Время завершения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.duration is 'Длительность';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.end_action is 'Действие на финише';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.res is 'Результат (0/1)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.msg is 'Сообщение об ошибке';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.last is 'Последнее успешное время';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log.value is 'Результат (JSON)';