CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_stat(log_id integer, log_tb text, period_date text DEFAULT NULL::text, load_date text DEFAULT NULL::text, key_date text DEFAULT NULL::text) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare 
    exe text = '';
    sql_txt text;
    rec record;
    ztest json;
    key date;
    keys date[];
begin
    period_date = coalesce(period_date, 'null');
    load_date = coalesce(load_date, 'now()');
    key_date = coalesce(key_date, 'null');
    
    exe = format($sql$
        insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_stat 
        (log_id, wf_obj, rw_cnt, data_min, data_max, load_min, load_max, key_min, key_max, data_name, load_name, key_name) 
         select %1$s, %2$L, count(1), 
         min(%3$s::timestamp),
         max(%3$s::timestamp),
         min(%4$s::timestamp),
         max(%4$s::timestamp),
         min(%5$s),
         max(%5$s),
         nullif(%3$L,'null'), %4$L, nullif(%5$L,'null')
         from %2$s as a
    $sql$, coalesce(log_id,0), log_tb, period_date, load_date, key_date);

    exe = format($sql$
        with add as (
            %s
            returning *
        )
        select add.* from add limit 1
    $sql$, exe);
    execute exe into rec;
    
    -- key = coalesce(s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp(rec.key_max)::date, rec.load_max::date, now()::date)::date;
    -- ztest = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(log_tb::text, key::date, rec.rw_cnt::int8, log_id::int4, clock_timestamp()::timestamp);

    keys = array [s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp(rec.key_max)::date, rec.load_max::date, rec.data_max::date, now()::date];
    ztest = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(log_tb::text, keys::date[], rec.rw_cnt::int8, log_id::int4, now()::timestamp);
    
    -- if not (ztest->>'ztest')::bool and (ztest->>'back')::bool then
    --     raise exception using ERRCODE = 'XX001', MESSAGE = format('Ztest error: %s', ztest::text), DETAIL = ztest::text, HINT = '';
    -- end if;
    
    return format('OK %s', ztest::text);

exception when OTHERS then
    declare 
        e_txt text;
        e_detail text;
        e_hint text;
        e_context text;
    begin
        get stacked diagnostics e_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
        
        if sqlstate = 'XX001' then
            raise exception using ERRCODE = sqlstate, MESSAGE = e_txt, DETAIL = e_detail, HINT = e_hint;
        end if;
        
        if split_part(e_context, 'statement\n', 1) like '% at EXECUTE %' then
            e_detail = exe;
            e_context = replace(e_context, exe, '...');
        end if;
        
        perform pg_notify('hr_edp', exe::text);
        perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(0, e_txt, e_detail, e_hint, e_context) ; --ЛОГИРОВАНИЕ
        return e_txt;
    end;
end;

$body$
EXECUTE ON ANY;
	