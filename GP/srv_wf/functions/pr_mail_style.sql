CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_style(add_st json DEFAULT NULL::json, ret_sql boolean DEFAULT false) 
	RETURNS json
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    style json;
    exe text;
    grp text;
    max int;
begin
    if ret_sql and add_st is not null then
        style = null;
    else
        style = $$
        {
            "th": { 
                "id": "th", 
                "style": { 
                    "text-align:": "center",
                    "background:": {
                        "(row->>'nn')::int = 0": "Gainsboro", 
                        "else": "silver"
                    }
                } 
            },
            "td": {
                "id": { "": { "type = 'number' or key in ('cnt')": "num", "else": "td" } },
                "style":{
                    "color:": {
                        "type = 'boolean' and value='true'": "green",
                        "type = 'boolean' and value='false'": "red"
                    },
                    "font-weight:": {
                        "type = 'boolean'": "600",
                        "(row->>'nn')::int = 0": "600"
                    },
                    "background:": {
                        "key in ('key_date')": {
                            "current_date - value::timestamp::date > 5": "salmon",
                            "current_date - value::timestamp::date > 3": "pink",
                            "current_date - value::timestamp::date > 1": "LemonChiffon"
                        },
                        "key in ('duration','max_duration')": {
                            "value >= '02:30'": "red",
                            "value >= '02:00'": "salmon",
                            "value >= '01:30'": "pink",
                            "value >= '01:00'": "yellow",
                            "value >= '00:30'": "LemonChiffon"
                        },
                        "key in ('res','res_msg')": {
                            "(row->>'res')::int = 1": "lime", 
                            "(row->>'res')::int = 0": "LemonChiffon", 
                            "(row->>'res')::int = -1": "yellow", 
                            "(row->>'res')::int = -2": "pink", 
                            "(row->>'res')::int = -3": "fuchsia", 
                            "(row->>'res')::int = -4": "skyblue", 
                            "(row->>'res')::int = -5": "violet", 
                            "(row->>'res')::int = -6": "gray", 
                            "(row->>'res')::int = -7": "silver", 
                            "(row->>'res')::int = -8": "orange", 
                            "(row->>'res')::int = -9": "salmon" 
                        },
                        "key in ('status_time')": {
                            "value::interval > '48 hours'::interval": "salmon",
                            "value::interval > '24 hours'::interval": "pink",
                            "value::interval > '12 hours'::interval": "gold",
                            "value::interval > '6 hours'::interval": "yellow",
                            "value::interval > '3 hours'::interval": "LemonChiffon"
                        },
                        "(row->>'nn')::int = 0": "Gainsboro",
                        "else": {
                            "key = 'cnt'": "silver",
                            "key = 'cnt_ok'": "lime",
                            "else": null
                        }
                    }
                }
            }
        }
        $$::json;
    end if;

    drop table if exists jsn;
    create temp table jsn
    WITH (appendonly=true,orientation=column, compresstype=zstd)
    on commit drop
    as
        with recursive jsn_u as (
            select ord 
                , 0 as lvl
                , nn
                , '' as p_key
                , key as full_key
                , key
                , json_typeof(value::json) type
        --        , value#>>'{}' value
                , value::json
            from (
                select 1 ord, * from json_each_text(style) with ordinality as j (key, value, nn)
                union 
                select 0 ord, * from json_each_text(add_st) with ordinality as j (key, value, nn)
            ) a
            union all
            select ord 
                , lvl + 1 as lvl
                , jd.nn
                , j.full_key as p_key
                , concat(full_key, '->', coalesce(jd.key, ja.nn::text)) as full_key
                , coalesce(jd.key, ja.nn::text) key
                , json_typeof(coalesce(jd.value, ja.value)) type
        --        , coalesce(jd.value::json , ja.value::json )#>>'{}' value
                , coalesce(jd.value::json , ja.value::json ) value
            from jsn_u j
            left join json_each(case j.type when 'object' then j.value::json else null end) with ordinality as jd (key, value, nn) on j.type = 'object'
            left join json_array_elements(case j.type when 'array' then j.value::json else null end) with ordinality as ja(value, nn) on j.type = 'array'    
            where j.type in ('object', 'array')
        ), jsn as (
            select distinct on (case when a.lvl <= 3 then a.full_key else concat(a.p_key,'>', a.nn) end) --*
                a.lvl, a.nn, a.ord, a.p_key, a.full_key, a.key ,a.type
                , case when a.type = 'object' then null else a.value end value
            from jsn_u a
            inner join (
                select distinct on (p_key) *
                from (
                    select p_key, ord, max(nn)
                    from jsn_u
                    group by 1, 2
                ) a
                order by p_key, ord
            ) b on (b.p_key = a.p_key and a.nn <= b.max) or a.lvl <= 3
            order by case when a.lvl <= 3 then a.full_key else concat(a.p_key,'>', a.nn) end, ord
        )
        select * from jsn order by lvl,p_key, nn
    DISTRIBUTED BY (p_key);

    max = (select max(lvl) from jsn);

    drop table if exists tmp_jsn;
    create temp table tmp_jsn
    WITH (appendonly=true,orientation=column, compresstype=zstd)
    on commit drop
    as select null::text g_key, null::text sql
    DISTRIBUTED BY (g_key);

    for k in 0..max loop

        exe = case
        when ret_sql and max-k = 0 then $$json_object_agg(key, coalesce(sql, value#>>'{}', value::text) order by nn)::text$$
        when ret_sql and max-k = 1 then $$
            string_agg(
                concat('select ', quote_literal(j.key), ', ', coalesce(sql, quote_literal(value#>>'{}'), value::text))
            , chr(10)||'union'||chr(10))
        $$
        when ret_sql and max-k = 2 then $$
            string_agg(
                coalesce(quote_literal(key)||'||'||sql||concat(' -- ord:', ord, ' nn:', nn, ' lvl:', lvl)
                    , quote_literal(concat(key, coalesce(value#>>'{}', value::text)))) 
            , ';') 
        $$
        when ret_sql then $$
            concat(chr(10), repeat(chr(9), lvl-2), 'case ', string_agg(
                concat(chr(10),repeat(chr(9), lvl-2), case when key = 'else' then 'else ' else 'when ' || key || ' then ' end
                    , coalesce(sql, quote_literal(value#>>'{}'), value::text))||concat(' -- ord:', ord, ' nn:', nn, ' lvl:', lvl)
            , ' ' order by nn), chr(10), repeat(chr(9), lvl-2), 'end')
        $$
        else 'json_object_agg(key, coalesce(sql::json, value::json) order by nn)::text'
        end;

        grp = case
        when ret_sql and max-k = 2 then 'nn*10 + ord'
        else 'lvl'
        end;
    
        drop table if exists tmp_jsn_tmp;
        
        exe = format($$
        create temp table tmp_jsn_tmp
        WITH (appendonly=true,orientation=column, compresstype=zstd)
        on commit drop
        as
            select p_key as g_key, %3$s as gr, %2$s sql
            from jsn j
            left join tmp_jsn b on full_key=g_key
            where j.lvl = %1$s and coalesce(value#>>'{}', b.sql) is not null and type <> 'null'
            group by 1, 2
        DISTRIBUTED BY (g_key)
        $$, max-k, exe, grp);
        raise info '%', exe;
        execute exe;

        drop table if exists tmp_jsn;
        create temp table tmp_jsn
        WITH (appendonly=true,orientation=column, compresstype=zstd)
        on commit drop
        as select * from tmp_jsn_tmp
        DISTRIBUTED BY (g_key);

    end loop;

    style  = (select sql from tmp_jsn limit 1);

    return style;
end;

$body$
EXECUTE ON ANY;
	