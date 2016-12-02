-- query for avg and std rank into window time with keyword terms

select avg(into_percent) as avg_into_percent,std(into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131106_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131106_event_bt where event_date in 
(select event_date from 20131106_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') 


select avg(into_percent) as avg_into_percent,std(into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131107_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131107_event_bt where event_date in 
(select event_date from 20131107_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') 


select avg(into_percent) as avg_into_percent,std(into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131108_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131108_event_bt where event_date in 
(select event_date from 20131108_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') 


select avg(into_percent) as avg_into_percent,std(into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131109_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131109_event_bt where event_date in 
(select event_date from 20131109_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') 


select avg(into_percent) as avg_into_percent,std(into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131110_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131110_event_bt where event_date in 
(select event_date from 20131110_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') 


-- query for stadistic for all union table

select avg(avg_into_percent), avg(std_into_percent), avg(total_windows), avg(avg_total_terms)
from 
(select avg(t11.into_percent) as avg_into_percent,std(t11.into_percent) as std_into_percent,
count(*) as total_windows, avg(total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131106_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131106_event_bt where event_date in 
(select event_date from 20131106_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%')  as t11

UNION ALL

select avg(t12.into_percent) as avg_into_percent,std(t12.into_percent) as std_into_percent,
count(*) as total_windows, avg(t12.total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131107_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131107_event_bt where event_date in 
(select event_date from 20131107_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') as t12

UNION ALL

select avg(t13.into_percent) as avg_into_percent,std(t13.into_percent) as std_into_percent,
count(*) as total_windows, avg(t13.total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131108_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131108_event_bt where event_date in 
(select event_date from 20131108_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') as t13

UNION ALL

select avg(t14.into_percent) as avg_into_percent,std(t14.into_percent) as std_into_percent,
count(*) as total_windows, avg(t14.total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131109_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131109_event_bt where event_date in 
(select event_date from 20131109_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') as t14

UNION ALL

select avg(t15.into_percent) as avg_into_percent,std(t15.into_percent) as std_into_percent,
count(*) as total_windows, avg(t15.total_terms) as avg_total_terms from
(select (t1.rank/t2.total_terms) as into_percent,t2.total_terms,t1.* from 20131110_event_bt as t1 
join  
(select event_date, count(*) as total_terms from 20131110_event_bt where event_date in 
(select event_date from 20131110_event_bt where term like '%philippines%' or term like '%typhoon%')
group by event_date) as t2 on t1.event_date = t2.event_date
where t1.term like '%philippines%' or t1.term like '%typhoon%') as t15
) as tf;
