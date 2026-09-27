CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids AS
 SELECT a.wf_name,
    a.id,
    a.completed,
    a.ts,
    a.keys_cnt,
    a.cnt,
    a.sum_len,
    a."time",
    a.wf_keys,
    (((NULLIF(b.alive, 'ACTIVE'::text) = ANY (ARRAY['COMPLETED'::text, 'ABORTED'::text])) AND ((b.status <> 'SUCCESS'::text) OR (NOT a.completed))) OR ((NOT a.completed) AND (a.ts < (now() - '00:30:00'::interval)))) AS retry,
    b.status_dttm,
    (b.auto)::boolean AS auto,
    b.alive,
    b.status,
    b.status_log,
    b.msg
   FROM (( SELECT a_1.wf_name,
            a_1.id,
            (min((a_1.completed)::integer))::boolean AS completed,
            max(a_1.ts) AS ts,
            count(a_1.wf_key) AS keys_cnt,
            sum(a_1.cnt) AS cnt,
            sum(a_1.sum_len) AS sum_len,
            max(a_1."time") AS "time",
            json_agg(a_1.wf_key ORDER BY a_1.wf_key DESC) AS wf_keys
           FROM s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log a_1
          GROUP BY a_1.wf_name, a_1.id) a
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading b ON ((b.id = a.id)))
  ORDER BY a.wf_name DESC, a.id DESC;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids is '
Сводка по пакетам обмена с привязкой к статусу загрузки.
Показывает признак завершения, количество ключей и необходимость повтора.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.wf_name is 'Имя потока';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.id is 'ID пакета';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.completed is 'Флаг: все ключи переданы';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.ts is 'Время последней передачи';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.keys_cnt is 'Число ключей в пакете';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.cnt is 'Общее число строк';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.sum_len is 'Общий объём данных';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.time is 'Макс. длительность';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.wf_keys is 'Список ключей (JSON)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.retry is 'Требуется повтор';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.status_dttm is 'Время последнего статуса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.auto is 'Автоматический режим';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.alive is 'Текущий статус процесса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.status is 'Результат выполнения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.status_log is 'Лог статуса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_ids.msg is 'Дополнительное сообщение';