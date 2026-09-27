CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_ztable (
	zscore numeric null,
	p_value numeric null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (zscore);