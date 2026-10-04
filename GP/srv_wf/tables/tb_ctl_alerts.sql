CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts (
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	wf_id bigint null,
	wf_name text null,
	alert_grp text null,
	alert_key text null,
	period_ts timestamp without time zone null,
	res integer null,
	msg text null,
	jsn json null,
	close_ts timestamp without time zone null
)
WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED RANDOMLY;
comment on table s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts is 'Заведённые SLA алерты потоков CTL. v1.3, 2026-09-09';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.ts is 'Время заведения алерта';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_id is 'Идентификатор потока в CTL';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_name is 'Имя потока';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_grp is 'Группа алерта';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_key is 'Ключ правила';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.period_ts is 'Начало периода правила';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.res is 'Код результата';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.msg is 'Причина алерта';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.jsn is 'Правило и замер';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.close_ts is 'Время закрытия: правило отработало без алерта';