CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.is_valid_json(json_text text) 
	RETURNS boolean
	LANGUAGE plpgsql
	VOLATILE
as $body$


begin
    perform jsonb(json_text);
    return true;
exception
    when others then
        return false;
end; 

$body$
EXECUTE ON ANY;
	