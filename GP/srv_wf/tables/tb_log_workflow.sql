CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow (
	id integer not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_id_seq'::regclass),
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	parent integer null,
	wf_action text null,
	wf_message text null
)
WITH (appendonly=false)
DISTRIBUTED BY (id);