CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_alerts(grp text DEFAULT NULL::text, bck_end time without time zone DEFAULT '12:00:00'::time without time zone, hist interval DEFAULT '1 day'::interval) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

-- E360-6367. SLA алерты потоков CTL.
-- 2026-10-08 13:20 MSK, v1.21, Чуркин Николай
--
-- Механизм общий: правило вешается на любой поток CTL. Тикет пришёл от Пакетной
-- выгрузки, но ни функция, ни таблица к ней не привязаны - не сужайте описание обратно.
--
-- Функцию зовёт CTL раз в 15 минут. Появился новый алерт - возвращаем res = -6 и отчёт;
-- письмо по statusNotifications рассылает сам CTL. Почту отсюда не шлём: в Greenplum нет
-- SMTP, и всё семейство pr_mail_* лишь собирает HTML (pr_send_mail тоже ничего не
-- отправляет, имя историческое).
--
-- Правила живут в параметрах потоков CTL и читаются из vw_log_ctl_wf - так же, как эта
-- вьюха достаёт из параметров wf_interval. Проброс через pr_swf_start_ctl не нужен.
--   wf_alert_group - имя группы, по нему фильтрует аргумент grp. Параметра нет - группа
--                    пустая строка, а не NULL, поэтому такие потоки берутся вызовом
--                    pr_mail_ctl_alerts(''). Аргумент NULL - все группы разом;
--   wf_alert       - КОГДА проверять: вид, дедлайн, номер статистики;
--   wf_alert_data  - ЧТО сверять в данных. Параметр необязательный, и само его наличие
--                    включает проверку данных. Нет его - смотрим только событие.
--
--   Отсюда три режима:
--     1. дедлайн + данные  - есть wf_alert_data: к дедлайну нужны данные не старше
--                            "дедлайн - lag";
--     2. дедлайн + событие - wf_alert_data нет: к дедлайну поток должен был успешно
--                            отработать (или отдать statval stat), бизнес-дату не смотрим;
--     3. от прошлого запуска - every/kind интервалом: календаря нет, считаем от последнего
--                            успешного запуска (или statval stat); wf_alert_data здесь не смотрится.
--
--   wf_alert. kind и every - синонимы: слово или интервал.
--     {"every":"daily",     "at":"09:00"}               -- каждый день
--     {"every":"workdays",  "at":"13:00"}               -- ПН-ПТ
--     {"every":"weekly",    "at":"09:00"}               -- по ПН (dow по умолчанию 1)
--     {"every":"weekly",    "at":"09:00", "dow":[1,3,5]}  -- ПН, СР, ПТ; 7 = воскресенье
--     {"every":"monthly",   "day":20}
--     {"every":"quarterly"}                             -- последнее число квартала
--     {"every":"yearly",    "month":12, "day":16}
--     {"every":"hourly"}                                -- = "1 hour", скользящее окно
--     {"every":"2 day",     "at":"09:00"}               -- через 2 дня после прошлого запуска
--     {"every":"1 day",     "stat":12}                  -- скользящее окно по 12 статистике
--     {"kind":"heartbeat",  "every":"1 hour"}           -- старая запись, работает
--     Слова нет вовсе - вид складывается из полей: {"at":"09:00"} - каждый день,
--     {"dow":[1,3,5]}, {"day":"last"}, {"month":[3,6,9,12], "day":"last"}.
--     dow, day и month - число или массив, условия через И, уточняют календарь слова.
--     stat не задан - поток отработал = загрузка alive COMPLETED и status SUCCESS, время end_dttm;
--     stat задан - публикация statval этого номера. При интервале dow, day и
--     month не смотрятся. Неизвестное слово - правило пропускается (skipped).
--     Интервал с at (от суток) - отсчёт от дня последнего запуска: отработал 3-го в любое время,
--     every "2 day", at "09:00" - к 5-му 09:00 обязан отработать снова. Не отрабатывал
--     никогда - алерт сразу. every без at -
--     скользящее окно: с прошлого запуска (или statval stat) прошло не больше every.
--
--   wf_alert_data:
--     {"lag":"1 day",        "obj":"stg.tb_incidentsm1"}      -- короткая схема
--     {"lag":"1 day",        "obj":"s_grnplm_vd_hr_edp_stg.tb_incidentsm1"}  -- и полная
--     {"lag":"1 mon 19 day", "obj":"..."}                     -- месячное, см. ниже
--     {"lag":"0 day",        "obj":"...", "field":"key_max"}
--     {"lag":"3 day",        "obj":"...", "expr":"max(report_dt)"}
--     obj   - откуда мерить свежесть, обязателен. Схему можно писать коротко:
--             "stg.tb_addresses" равносильно "s_grnplm_vd_hr_edp_stg.tb_addresses";
--     lag   - НА СКОЛЬКО ДАННЫМ РАЗРЕШЕНО ОТСТАВАТЬ ОТ ДЕДЛАЙНА, то есть Т-N из тикета;
--             по умолчанию "0 day" - данные за сам день дедлайна;
--     field - колонка tb_log_workflow_stat, по которой меряем (по умолчанию data_max);
--     expr  - выражение бизнес-даты прямо по объекту, если поток не идёт через движок и
--             строк в tb_log_workflow_stat у него нет (ue_aimodel_rating, vw_predict_buckets).
--
--   Проверка данных считается так:
--       need  = дедлайн - lag              -- дата, за которую данные обязаны быть
--       алерт = последняя бизнес-дата объекта < need
--   Пример: wf_alert {"at":"09:00"} и wf_alert_data {"lag":"1 day",...} -
--   сегодня в 09:00 обязаны быть данные за вчера; лежат за позавчера - алерт.
--   lag НЕ задаёт, как часто проверять: это делают every/kind, dow/day/month и at.
--
--   at по умолчанию - 23:59:59, конец дня дедлайна: "по ПН" без at - до конца понедельника,
--   day 31 - до конца 31-го.
--
--   Режим события: дедлайн закрывает запуск в ДЕНЬ дедлайна или позже. Раньше - не
--   считается: у правила "по ПН" запуск в субботу ничего не закрывает, и повторный
--   запуск во вторник следующий понедельник тоже не закроет. Ежедневно к 09:00: запуск в
--   10:00 - опоздание, алерт гаснет; к завтрашним 09:00 нужен запуск завтра. Пропустил
--   день и отработал назавтра утром - старый алерт гаснет тоже, событие новее дедлайна.
--   Дедлайн ровно в 00:00 - конец прошлого дня: запуск накануне его закрывает.
--
--   Осторожно с месячными и квартальными объектами. Если бизнес-дата у них - метка периода
--   (первое число месяца), сравнение идёт с меткой, а не с днём загрузки, и lag надо
--   доводить до неё. "К 20 числу нужен прошлый месяц" - это lag "1 mon 19 day"
--   (20 сентября минус столько = 1 августа), а не "11 day": с ним need упрётся в 9 сентября,
--   августовская метка окажется старше, и алерт будет срабатывать каждый месяц впустую.
--
-- Cron не используем намеренно: в plpgsql пришлось бы писать свой разбор, а "последнее
-- число квартала" им всё равно не выражается. Явные поля покрывают весь список тикета.
--
-- Дедупликация: на один алерт реагируем не чаще раза в сутки в пределах периода правила.
-- Ключ - тройка (wf_id, alert_key, period_ts) в tb_ctl_alerts плюс сутки от ts последней
-- строки. Прошёл следующий период - появляется новая строка и новая реакция; период
-- длиннее суток (weekly, monthly, quarterly, yearly, выходные у workdays) - реакция
-- повторяется каждые сутки, пока проблема жива, иначе о ней напомнили бы только через
-- неделю или месяц.
--
-- Закрытие: правило, отработавшее в этом вызове без алерта, гасит свои открытые строки -
-- в close_ts проставляется время. Оно и останавливает суточные повторы. Закрываются все
-- открытые строки правила, включая прошлые периоды. Само правило при этом не считается
-- отработавшим, если было пропущено (битый JSON, неизвестный вид, кривой obj):
-- непроверенное правило ничего не закрывает.
--
-- В отчёт идут ВСЕ события за окно hist, свежие сверху, - и открытые, и закрытые, с
-- временем закрытия в колонке closed. Окно по умолчанию сутки; шире - аргументом,
-- он же правится в параметре потока wf_exe без выкладки функции:
--   pr_mail_ctl_alerts(null, '12:00', '7 days')
--
-- Воскресенье: с 00:00 до bck_end идёт BACKUP GP, данные не обновляются - алерты в это
-- окно не заводим вовсе. Границу правят в параметре потока wf_exe, без выкладки функции:
--   pr_mail_ctl_alerts(null, '18:00')
--
-- obj и expr подставляются в динамический SQL - как tbl/bdate в pr_check_bd4ds. obj
-- обязан выглядеть как схема.таблица, а expr не должен содержать ';': правило, которое
-- этого не проходит, пропускается и попадает в счётчик skipped. Это защита от опечатки и
-- от многооператорного текста, а не от злого умысла: expr по замыслу произвольное
-- выражение бизнес-даты, и настоящая граница доверия - у кого есть права править
-- параметры потоков в CTL.
--
-- Время серверное: и now(), и граница bck_end берутся в часовом поясе сессии. Если
-- сервер живёт не в московском времени, границу задавать с поправкой.

declare 
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;

    sql text;
    log_id int4;
    mail_id int4;
    end_id int4;
    m_res int4 = 1;
    new_cnt int4 = 0;
    bad_cnt int4 = 0;
    closed_cnt int4 = 0;

    r record;
    r_jsn json;
    d_jsn json;
    r_kind text;
    r_at time;
    r_lag interval;
    r_every interval;
    r_dow int4[];
    r_day int4[];
    r_month int4[];
    r_last boolean;
    r_key text;
    r_src text;
    r_obj text;
    r_msg text;

    last_dt timestamp;
    need_dt date;
    prev_ts timestamp;
    per_ts timestamp;

    style json;
    html text;
    mail_txt text;
    m_jsn json;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('ALERTS (pr_mail_ctl_alerts %s)', coalesce(grp, 'all')));
    begin
        -- Воскресный бэкап: молчим, ничего не заводя.
        if extract(dow from now()) = 0 and now()::time < bck_end then
            m_txt = format('backup window till %s', bck_end);
            log_id = pr_log_action('end', m_txt, log_id);
            return json_build_object('res', 1, 'msg', m_txt)::text;
        end if;

        -- Правила из параметров потоков CTL. "Поток активен" читаем как "не удалён":
        -- alive описывает загрузку, а не поток, и к настройке алерта отношения не имеет.
        -- Потоки без обоих параметров отсеиваются сразу, во внешнем where: в CTL их сотни,
        -- а с настроенным алертом - десятки, и тащить остальные во временную таблицу незачем.
        drop table if exists tmp_alert_rule;
        create temp table tmp_alert_rule on commit drop as
        select * from (
            select a.id as wf_id
                 , a.name as wf_name
                 , (select j.value->>'prior_value' from jsonb_array_elements(a.msg->'wf'->'param') j
                     where j.value->>'param' = 'wf_alert' limit 1) as rule_txt
                 , (select j.value->>'prior_value' from jsonb_array_elements(a.msg->'wf'->'param') j
                     where j.value->>'param' = 'wf_alert_data' limit 1) as data_txt
                 , coalesce((select j.value->>'prior_value' from jsonb_array_elements(a.msg->'wf'->'param') j
                     where j.value->>'param' = 'wf_alert_group' limit 1), '') as alert_grp
            from vw_log_ctl_wf a
            where coalesce(a.deleted, false) = false
        ) a
        where rule_txt is not null or data_txt is not null
        distributed randomly;

        -- Чужие группы отсекаем ПЕРЕД подсчётом битых правил: wf_alert_group - отдельный
        -- параметр и от валидности JSON не зависит, поэтому опечатка в правиле соседней
        -- группы известна кому надо, а в наше письмо "N rule(s) skipped" попадать не должна.
        -- Группа нормализована при сборе, поэтому сравнение прямое: вызов с '' берёт
        -- ровно те потоки, у которых wf_alert_group не задан.
        delete from tmp_alert_rule where grp is not null and alert_grp <> grp;

        -- Нечитаемое правило молча пропадать не должно: считаем и показываем в msg,
        -- иначе опечатка в параметре выглядит как "алертов нет". Пустой rule_txt здесь
        -- значит "задан wf_alert_data, а само wf_alert забыли": потоки вовсе без настройки
        -- отсеяны выше, при наполнении. Считаем и удаляем одним оператором - разными они
        -- разъезжались бы при любой правке условия.
        delete from tmp_alert_rule
         where rule_txt is null or not is_valid_json(rule_txt)
            or (data_txt is not null and not is_valid_json(data_txt));
        get diagnostics bad_cnt = ROW_COUNT;

        drop table if exists tmp_alert_new;
        create temp table tmp_alert_new (
            wf_id bigint, wf_name text, alert_grp text, alert_key text,
            period_ts timestamp, msg text, jsn json
        ) on commit drop distributed randomly;

        -- Правила, отработавшие в этом вызове чисто: замер сделан, претензий нет. Ими
        -- гасятся активные алерты. Пропущенные правила (битый JSON, неизвестный вид,
        -- кривой obj) сюда не попадают - непроверенное правило ничего не закрывает.
        drop table if exists tmp_alert_ok;
        create temp table tmp_alert_ok (
            wf_id bigint, alert_key text
        ) on commit drop distributed randomly;

        -- Текущий ключ каждого проверенного правила. По нему гасятся открытые строки потока
        -- со старым ключом: правило переписали или сменился формат ключа - иначе их закрыть нечем.
        drop table if exists tmp_alert_cur;
        create temp table tmp_alert_cur (
            wf_id bigint, alert_key text
        ) on commit drop distributed randomly;

        for r in select wf_id, wf_name, rule_txt, data_txt, alert_grp from tmp_alert_rule order by wf_name loop
            -- Каждое правило - в своём блоке. Правило, на котором падает запрос (obj ссылается на
            -- удалённую таблицу, lag или at не приводятся к типу, expr с ошибкой), раньше роняло
            -- весь вызов: отчёт уходил с -9, и алертов не было видно ни по одному потоку.
            -- Теперь такое правило само становится алертом с ключом rule error: владелец видит
            -- его в письме своей группы. В tmp_alert_ok оно не попадает - непроверенное правило
            -- ничего не закрывает. Блок с exception в GP - подтранзакция на правило; правил
            -- десятки, это дёшево.
            begin
                sql = null;         -- в jsn ошибки должен попасть запрос этого правила
                r_jsn = r.rule_txt::json;
                last_dt = null;
                -- kind и every - синонимы. Значение - слово (daily, weekly, ...) или интервал
                -- ("2 day", "1 hour"). Старая запись {"kind":"heartbeat","every":"1 hour"}
                -- задаёт оба поля: heartbeat значит "смотри интервал", по умолчанию сутки.
                r_kind = lower(coalesce(nullif(r_jsn->>'kind', ''), nullif(r_jsn->>'every', ''), ''));
                if r_kind = 'heartbeat' then
                    r_kind = coalesce(nullif(r_jsn->>'every', ''), '1 day');
                elsif r_kind = 'hourly' then
                    r_kind = '1 hour';
                end if;

                -- Когда поток отработал последний раз. stat не задан - по успешно закрытой
                -- загрузке: alive COMPLETED и status SUCCESS, момент - end_dttm. Незакрытая
                -- загрузка не в счёт, даже в SUCCESS: пока она висит, следующая не стартует.
                -- ERROR данных не дал, хоть загрузку и закрывают как COMPLETED. stat задан -
                -- по публикации statval этого номера. Режим данных перезапишет last_dt своим замером.
                if (r_jsn->>'stat') is null then
                    r_src = 'run';
                    select max(l.end_dttm) into last_dt
                    from vw_log_ctl_loading l
                    where l.wf_id = r.wf_id
                      and l.alive = 'COMPLETED'
                      and l.status = 'SUCCESS';
                else
                    r_src = 'stat ' || (r_jsn->>'stat');
                    select max(a.ts) into last_dt
                    from tb_log_ctl a
                    join vw_log_ctl_loading l on l.id = a.id
                    where a.obj = 'statval'
                      and (a.msg->>'stat_id')::int4 = (r_jsn->>'stat')::int4
                      and l.wf_id = r.wf_id;
                end if;

                if r_kind ~ '^ *[0-9]' then
                    r_every = r_kind::interval;

                    if nullif(r_jsn->>'at', '') is null or r_every < interval '1 day' then
                        -- Без at (и для интервалов короче суток, где at смысла не имеет) -
                        -- скользящее окно. Окно хартбита прибито к сетке, иначе период "плыл" бы от вызова к вызову.
                        per_ts = to_timestamp(floor(extract(epoch from now()) / extract(epoch from r_every))
                                              * extract(epoch from r_every))::timestamp;
                        r_key = format('%s / %s', r_src, r_every);
                        r_at = null;
                    else
                        -- С at дедлайн - день последнего срабатывания плюс every, к часу at.
                        -- Период - сам дедлайн: он не меняется, пока поток снова не отработает.
                        r_at = (r_jsn->>'at')::time;
                        per_ts = coalesce((last_dt + r_every)::date + r_at, current_date::timestamp);
                        r_key = format('%s / %s at %s', r_src, r_every, r_at);
                    end if;

                    if last_dt is null
                       or (r_at is null and last_dt < now() - r_every)
                       or (r_at is not null and per_ts <= now()) then
                        r_msg = case when (r_jsn->>'stat') is null
                            then format('нет успешного запуска за %s, последний %s'
                                , r_every, coalesce(left(last_dt::text, 19), 'никогда'))
                            else format('нет статистики %s за %s, последняя %s'
                                , r_jsn->>'stat', r_every, coalesce(left(last_dt::text, 19), 'никогда'))
                            end;
                        insert into tmp_alert_new
                        values (r.wf_id, r.wf_name, r.alert_grp, r_key, per_ts, r_msg
                              , json_build_object('rule', r_jsn, 'last', left(last_dt::text, 19)));
                    else
                        insert into tmp_alert_ok values (r.wf_id, r_key);
                    end if;
                else
                    r_at = coalesce(nullif(r_jsn->>'at', ''), '23:59:59')::time;
                    -- dow, day, month - число или массив; поля нет - условия нет. dow по isodow:
                    -- 1 = ПН ... 7 = ВС. day "last" - последнее число месяца.
                    r_last = coalesce((r_jsn->>'day') = 'last', false);
                    r_dow = case when (r_jsn->'dow') is null then null
                                 when json_typeof(r_jsn->'dow') = 'array'
                                 then array(select json_array_elements_text(r_jsn->'dow')::int4)
                                 else array[(r_jsn->>'dow')::int4] end;
                    r_day = case when (r_jsn->'day') is null or r_last then null
                                 when json_typeof(r_jsn->'day') = 'array'
                                 then array(select json_array_elements_text(r_jsn->'day')::int4)
                                 else array[(r_jsn->>'day')::int4] end;
                    r_month = case when (r_jsn->'month') is null then null
                                   when json_typeof(r_jsn->'month') = 'array'
                                   then array(select json_array_elements_text(r_jsn->'month')::int4)
                                   else array[(r_jsn->>'month')::int4] end;
                    -- Слово задаёт календарь по умолчанию, явные dow/day/month его уточняют.
                    -- Слова нет - вид складывается из самих полей, метка нужна только для
                    -- alert_key: ключи заведённых алертов не меняются.
                    if r_kind = '' then
                        r_kind = case when r_month is not null then 'yearly'
                                      when r_day is not null or r_last then 'monthly'
                                      when r_dow is not null then 'weekly'
                                      else 'daily' end;
                    elsif r_kind = 'workdays' then
                        r_dow = coalesce(r_dow, '{1,2,3,4,5}');
                    elsif r_kind = 'weekly' then
                        r_dow = coalesce(r_dow, '{1}');
                    elsif r_kind in ('monthly', 'yearly') then
                        if r_day is null and not r_last then r_day = '{1}'; end if;
                        if r_kind = 'yearly' then r_month = coalesce(r_month, '{1}'); end if;
                    elsif r_kind = 'quarterly' then
                        r_month = coalesce(r_month, '{3,6,9,12}');
                        r_last = r_last or r_day is null;
                    elsif r_kind <> 'daily' then
                        bad_cnt = bad_cnt + 1;   -- слово неизвестно
                        continue;
                    end if;

                    -- Последний наступивший дедлайн и предыдущий. Перебором по календарю: так
                    -- все виды, включая "последнее число квартала", считаются одной формулой.
                    -- 800 дней - две годовые отсечки назад: предыдущая нужна режиму события.
                    select max(dl) filter (where rn = 1), max(dl) filter (where rn = 2)
                    into per_ts, prev_ts
                    from (
                        select d + r_at as dl, row_number() over (order by d desc) as rn
                        from generate_series(current_date - 800, current_date, '1 day'::interval) d
                        where (r_dow   is null or extract(isodow from d)::int4 = any(r_dow))
                          and (r_day   is null or extract(day    from d)::int4 = any(r_day))
                          and (r_month is null or extract(month  from d)::int4 = any(r_month))
                          and (not r_last or d::date = (date_trunc('month', d) + interval '1 month' - interval '1 day')::date)
                          and d + r_at <= now()
                    ) a
                    where rn <= 2;

                    if per_ts is null then
                        bad_cnt = bad_cnt + 1;   -- поля не дают ни одного дедлайна (31 февраля)
                        continue;
                    end if;

                    if r.data_txt is null then
                        -- Режим события: дедлайн есть, бизнес-дату не смотрим. Спрашиваем только,
                        -- был ли запуск в день текущего дедлайна или позже. Запуск раньше дня
                        -- дедлайна (субботний у правила "по ПН", повтор после прошлого дедлайна)
                        -- не считается. Дедлайн в 00:00 относится к прошлому дню - минус секунда.
                        -- Параметра wf_alert_data нет - и проверять в данных нечего.
                        r_key = format('%s %s event %s', r_kind, r_at, r_src);
                        if last_dt is null or last_dt::date < (per_ts - interval '1 second')::date then
                            r_msg = case when (r_jsn->>'stat') is null
                                then format('к %s поток не отработал успешно, последний успех %s'
                                    , left(per_ts::text, 16), coalesce(left(last_dt::text, 19), 'никогда'))
                                else format('к %s поток не отдал статистику %s, последняя %s'
                                    , left(per_ts::text, 16), r_jsn->>'stat', coalesce(left(last_dt::text, 19), 'никогда'))
                                end;
                            insert into tmp_alert_new
                            values (r.wf_id, r.wf_name, r.alert_grp, r_key, per_ts, r_msg
                                  , json_build_object('rule', r_jsn, 'since', left(prev_ts::text, 16)
                                                    , 'last', left(last_dt::text, 19)));
                        else
                            insert into tmp_alert_ok values (r.wf_id, r_key);
                        end if;
                    else
                        -- Режим данных: что именно сверять, лежит в wf_alert_data.
                        d_jsn = r.data_txt::json;
                        r_lag = coalesce(nullif(d_jsn->>'lag', ''), '0 day')::interval;
                        if coalesce(d_jsn->>'obj', '') !~ '^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$'
                           or coalesce(d_jsn->>'expr', '') ~ ';' then
                            bad_cnt = bad_cnt + 1;
                            continue;
                        end if;
                        -- Схему можно писать коротко: stg.tb_addresses разворачивается в
                        -- s_grnplm_vd_hr_edp_stg.tb_addresses - тем же правилом, что sch в
                        -- pr_swf_start_ctl. Полное имя оставляем как есть. Сравнение по левым 19
                        -- символам, а не like: в like подчёркивание было бы шаблоном.
                        r_obj = d_jsn->>'obj';
                        if left(r_obj, 19) <> 's_grnplm_vd_hr_edp_' then
                            r_obj = 's_grnplm_vd_hr_edp_' || r_obj;
                        end if;
                        need_dt = (per_ts - r_lag)::date;

                        if nullif(d_jsn->>'expr', '') is not null then
                            sql = format('select (%s)::timestamp from %s', d_jsn->>'expr', r_obj);
                        else
                            sql = format('select max(%I)::timestamp from tb_log_workflow_stat where wf_obj = %L'
                                       , coalesce(nullif(d_jsn->>'field', ''), 'data_max'), r_obj);
                        end if;
                        execute sql into last_dt;

                        r_key = format('%s %s T-%s', r_kind, r_at, r_lag);
                        if last_dt is null or last_dt::date < need_dt then
                            r_msg = format('нет данных за %s, последние %s'
                                , need_dt, coalesce(left(last_dt::text, 19), 'никогда'));
                            insert into tmp_alert_new
                            values (r.wf_id, r.wf_name, r.alert_grp, r_key, per_ts, r_msg
                                  , json_build_object('rule', r_jsn, 'data', d_jsn, 'obj', r_obj
                                                    , 'need', need_dt, 'last', left(last_dt::text, 19)));
                        else
                            insert into tmp_alert_ok values (r.wf_id, r_key);
                        end if;
                    end if;
                end if;

                -- Правило проверено (алерт по данным или нет - неважно): запоминаем его текущий
                -- ключ. По нему гасятся открытые строки потока с другим ключом, в том числе
                -- прошлые "rule error" и строки до правки правила.
                insert into tmp_alert_cur values (r.wf_id, r_key);
            exception when OTHERS then
                get stacked diagnostics r_msg = MESSAGE_TEXT;
                insert into tmp_alert_new
                values (r.wf_id, r.wf_name, r.alert_grp, 'rule error', current_date::timestamp
                      , format('правило не проверено: %s', r_msg)
                      , json_build_object('rule', r.rule_txt, 'data', r.data_txt, 'sql', sql));
            end;
        end loop;

        -- Заводим только то, о чём в этих сутках ещё не сообщали. Отдельной отметки реакции
        -- нет: реакция - это возврат res = -6 из этого же вызова, и сам факт строки её
        -- означает.
        --
        -- Ключ прежний - (wf_id, alert_key, period_ts), но к нему добавлены сутки: у правил
        -- с периодом длиннее суток (weekly, monthly, quarterly, yearly, выходные у workdays)
        -- одной реакции на период мало - проблема живёт неделями, а письмо приходило бы раз
        -- в период. Теперь такой алерт повторяется каждые сутки, пока правило не отработает
        -- чисто. Суточным правилам это ничего не меняет: у них на следующие сутки и period_ts
        -- уже другой.
        insert into tb_ctl_alerts (ts, wf_id, wf_name, alert_grp, alert_key, period_ts, res, msg, jsn)
        select clock_timestamp(), a.wf_id, a.wf_name, a.alert_grp, a.alert_key, a.period_ts, -6, a.msg, a.jsn
        from tmp_alert_new a
        where not exists (
            select 1 from tb_ctl_alerts b
            where b.wf_id = a.wf_id and b.alert_key = a.alert_key and b.period_ts = a.period_ts
              and b.ts > now() - interval '1 day'
        );
        get diagnostics new_cnt = ROW_COUNT;

        -- Закрываем открытые строки двумя путями, период в ключе не участвует - гасятся и прошлые
        -- периоды. 1) Правило отработало чисто - его строки с тем же ключом (tmp_alert_ok), иначе
        -- строка висела бы в отчёте до конца окна hist и продолжала бы повторяться. 2) Правило
        -- проверено - строки его потока с другим ключом (tmp_alert_cur): правило переписали,
        -- сменился формат ключа или прошёл "rule error".
        -- Update здесь один на вызов и только по открытым строкам: в GP он держит их до
        -- конца транзакции, поэтому колонка и заполняется одним заходом, а не по алерту.
        update tb_ctl_alerts a
        set close_ts = clock_timestamp()
        where a.close_ts is null
          and (exists (select 1 from tmp_alert_ok o
                       where o.wf_id = a.wf_id and o.alert_key = a.alert_key)
               or exists (select 1 from tmp_alert_cur c
                          where c.wf_id = a.wf_id and c.alert_key <> a.alert_key));
        get diagnostics closed_cnt = ROW_COUNT;

        if new_cnt > 0 then
            m_res = -6;
            m_txt = format('%s new alert(s)', new_cnt);
        else
            m_txt = 'no new alerts';
        end if;
        if closed_cnt > 0 then
            m_txt = format('%s, %s closed', m_txt, closed_cnt);
        end if;
        if bad_cnt > 0 then
            m_txt = format('%s, %s rule(s) skipped', m_txt, bad_cnt);
        end if;

        -- В отчёт идут ВСЕ события за окно hist, свежие сверху, - и открытые, и уже
        -- закрытые: закрытая строка показывает, что проблема была и когда её сняли, а без
        -- неё письмо про соседний алерт выглядело бы так, будто утром ничего не падало.
        -- Окно по умолчанию сутки, поэтому история отчёт не раздувает.
        -- since - когда об этой проблеме сообщили впервые. Считается оконкой по всей таблице,
        -- а окно hist накладывается снаружи: иначе у алерта, который тянется дольше окна, в
        -- отчёте осталась бы только последняя строка и since совпал бы с ts.
        -- Серию задаёт пара (wf_id, alert_key) и close_ts, а period_ts в partition НЕ входит:
        -- у недельного правила через неделю метка периода другая, и отсчёт начался бы заново,
        -- хотя проблема никуда не делась. Обрывает серию только закрытие - после него
        -- вернувшаяся проблема считается новой, а у снятых строк остаётся их прежнее since.
        drop table if exists tmp_alerts;
        create temp table tmp_alerts on commit drop as
        select left(a.ts::text, 19) as ts
             , a.wf_name
             , a.alert_grp
             , a.alert_key
             , left(a.period_ts::text, 16) as period
             , left(a.since::text, 16) as since
             , left(a.close_ts::text, 19) as closed
             , a.msg
        from (
            select a.*
                 , min(a.ts) over (partition by a.wf_id, a.alert_key, a.close_ts) as since
            from tb_ctl_alerts a
            where grp is null or coalesce(a.alert_grp, '') = grp
        ) a
        where a.ts > now() - hist
        distributed randomly;

        -- ret_sql = true обязателен: без него pr_mail_style отдаёт стили вложенным
        -- JSON (формат pr_tbl2html_loop, она конвертирует их сама), а pr_tbl2html ждёт
        -- готовые SQL-фрагменты и падает на 'syntax error at or near "{"'.
        -- Раскраска. Добавка домешивается к общему стилю внутренним вызовом (ret_sql = false),
        -- внешний переводит результат в SQL - тем же порядком, каким это делает
        -- pr_tbl2html_loop. Ветки дописываются в case по background, дефолтные остаются.
        -- Скобки вокруг row->>'...' обязательны: в GP6 (Postgres 9.4) IS связывает сильнее
        -- оператора ->>, поэтому row->>'closed' is not null разбирается как
        -- row ->> ('closed' is not null) и падает с "operator does not exist: json ->> boolean".
        -- В Postgres 9.5 приоритеты поменяли, так что локальный стенд на PG16 это пропускает.
        -- Порядок веток здесь и есть приоритет:
        --   closed   - закрытое событие красим целиком, чтобы живое от снятого отделялось
        --              сразу; поэтому ветка первая и перебивает остальные;
        --   никогда  - данных по объекту нет вовсе или у потока нет ни одного успешного запуска (statval stat).
        --              Это почти всегда ошибка в самом правиле (не тот obj, не тот stat), а
        --              не сбой тракта, и чинить надо правило;
        --   period   - насколько просрочен дедлайн, шкала как у key_date в общем стиле;
        --   since    - сколько проблема уже тянется. Красится вся строка: то, о чём пишем
        --              третьи сутки, важнее свежего.
        style = pr_mail_style(pr_mail_style($${ "td": { "style": { "background:": {
              "(row->>'closed') is not null": "lightgreen"
            , "key = 'msg' and value like '%никогда%'": "salmon"
            , "key = 'period'": {
                  "current_date - value::timestamp::date > 5": "salmon"
                , "current_date - value::timestamp::date > 3": "pink"
                , "current_date - value::timestamp::date > 1": "LemonChiffon"
              }
            , "now() - (row->>'since')::timestamp > interval '3 days'": "salmon"
            , "now() - (row->>'since')::timestamp > interval '1 day'": "LemonChiffon"
        } } } }$$::json), true);
        html = format('<div style="color:%1$s"><h2> CTL Alerts %2$s </h2><h4> %3$s </h4></div>'
            , case when new_cnt > 0 then 'red' else 'green' end, coalesce(grp, 'all'), m_txt);
        html = concat(html, pr_tbl2html('tmp_alerts', 'CTL Alerts', 'order by ts desc, wf_name', style));

        mail_id = pr_swf_log_action('CTL Alerts', 'mail', json_build_object('len', length(html), 'html', html));
        end_id = pr_swf_log_action('end', 'mail', null, mail_id);
        mail_txt = pr_send_mail(mail_id::text);
        -- pr_send_mail при своей ошибке отдаёт голый текст, а не JSON. Разбирать его нечем,
        -- но терять из-за этого сам алерт нельзя: он уже заведён и отреагирован.
        if not is_valid_json(coalesce(mail_txt, '')) then
            m_txt = format('%s (mail: %s)', m_txt, left(coalesce(mail_txt, 'null'), 200));
            mail_txt = '{}';
        end if;

        -- res и msg свои, остальное - от pr_send_mail (id, ts, report, html).
        m_jsn = (
            select json_object_agg(key, value) from (
                select 'res' as key, to_json(m_res) as value
                union all select 'msg', to_json(m_txt)
                union all select * from json_each(mail_txt::json) where key not in ('res', 'msg')
            ) a
        );

        log_id = pr_log_action('end', format('%s, %s rules', m_txt, (select count(1) from tmp_alert_rule)), log_id);
        return m_jsn::text;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform pr_Log_error(log_id, m_txt, e_detail, sql, e_context) ; 
        return format('Error: %s', m_txt);
    end;
end;

$body$
EXECUTE ON ANY;

-- DEFAULT в сигнатуре COMMENT ON недопустим, как и в DROP FUNCTION — только типы.
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_alerts(text, time without time zone, interval) IS 'SLA алерты потоков CTL. v1.21, 2026-10-08';
