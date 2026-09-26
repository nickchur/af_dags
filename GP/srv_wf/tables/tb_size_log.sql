CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_size_log (
	load_date timestamp without time zone null,
	table_schema text null,
	table_name text null,
	table_size bigint null,
	relstorage character(1) null,
	reloptions text null,
	distributedby text null,
	partition_def text null,
	prt_end text null,
	n_live_tup bigint null,
	n_dead_tup bigint null,
	prt_cnt bigint null,
	use_cnt bigint null,
	prt_analyze bigint null,
	last_analyze text null,
	last_vacuum text null,
	tableowner name null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (table_schema, table_name);