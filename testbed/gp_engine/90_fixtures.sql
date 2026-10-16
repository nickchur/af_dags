-- Данные стенда для отчётов CTL: то, что на бою настраивают руками.
-- 2026-10-05 13:06 MSK · v1.0 · Nick Churkin · NSChurkin@sber.ru
--
-- tb_bd4ds: без строк группы 'all' pr_check_bd4ds('all') собирает пустой SQL
-- («as order by 1,2») и падает с «syntax error at or near order» (res -9).
-- Проверяем свежесть журнала эмулятора CTL — его пополняет каждый тестовый прогон.
delete from s_grnplm_vd_hr_edp_srv_wf.tb_bd4ds where grp = 'all' and sch = 'srv_wf' and tbl = 'tb_log_ctl';
insert into s_grnplm_vd_hr_edp_srv_wf.tb_bd4ds (grp, sch, tbl, whr, bdate, operation, condition)
values ('all', 'srv_wf', 'tb_log_ctl', null, 'max(ts)::date', '>=', 'current_date - 1');
