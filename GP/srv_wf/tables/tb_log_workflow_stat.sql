CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_stat (
	log_id integer not null,
	wf_obj text null,
	rw_cnt bigint null,
	data_min timestamp without time zone null,
	data_max timestamp without time zone null,
	load_min timestamp without time zone null,
	load_max timestamp without time zone null,
	data_name text null,
	load_name text null,
	key_name text null,
	key_min text null,
	key_max text null,
	ts timestamp without time zone null DEFAULT now()
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (log_id);