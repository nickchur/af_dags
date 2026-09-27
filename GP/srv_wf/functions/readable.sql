CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.readable(size numeric, base integer DEFAULT 1024) 
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

DECLARE
    units TEXT[];
    i INTEGER;
    size_value NUMERIC;
    sign TEXT := '';
    abs_size NUMERIC;
BEGIN
    -- Выбор единиц измерения
    IF base = 1024 THEN
        units := ARRAY['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    ELSE
        units := ARRAY['', 'тыс', 'млн', 'млрд', 'трлн', 'птлн'];
    END IF;

    -- Обработка нуля
    IF size IS NULL OR size = 0 THEN
        RETURN '0 ' || units[1];
    END IF;

    -- Обработка знака
    IF size < 0 THEN
        sign := '-';
        abs_size := abs(size);
    ELSE
        abs_size := size;
    END IF;

    -- Расчет индекса (floor(log(base, value)))
    i := floor(log(base, abs_size))::INTEGER;

    -- Ограничение по длине массива
    IF i >= array_length(units, 1) THEN 
        i := array_length(units, 1) - 1; 
    END IF;
    IF i < 0 THEN i := 0; END IF;

    -- Итоговое значение (индексация массивов в PG начинается с 1)
    size_value := round(abs_size / (base::NUMERIC ^ i), 1);

    RETURN sign || size_value::TEXT || ' ' || units[i + 1];
END;

$body$
EXECUTE ON ANY;
	