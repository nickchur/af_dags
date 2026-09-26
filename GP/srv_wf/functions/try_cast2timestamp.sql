CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp(cast_text text, debug boolean DEFAULT false) 
	RETURNS timestamp without time zone
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return nullif(cast_text,'')::timestamp; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::timestamp;
end;

$body$
EXECUTE ON ANY;
	