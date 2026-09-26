CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_action(v_action text, v_message text DEFAULT ''::text, log_id integer DEFAULT NULL::integer) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    new_id int4;
begin
    INSERT INTO s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow
    (parent, wf_action, wf_message) values (nullif(log_id,0), v_action, v_message)
    returning id into new_id ;  

    return new_id; 

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

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context); 
        return 0;
    end;
end; 
$body$
EXECUTE ON ANY;
	