CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_err (
	log_id integer not null,
	message text null,
	detail text null,
	hint text null,
	context text null,
	dt timestamp without time zone null DEFAULT clock_timestamp(),
	usename name null DEFAULT "current_user"()
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=7)
DISTRIBUTED BY (log_id);