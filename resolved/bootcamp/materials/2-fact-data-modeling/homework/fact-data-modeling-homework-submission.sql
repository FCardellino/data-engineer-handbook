
---------------------------------------------------------------------------------
--TASK 1: A query to deduplicate game_details from Day 1 so there's no duplicates
---------------------------------------------------------------------------------

--NOTE: In the CTE game_details_deduplicated I created the query to deduplicate the games_detail table
--then I added the test CTE where a kept the first row of each duplicated row from games_detail, and add a query that select from the test CTE to check that there are no duplicates 
with game_details_deduplicated as(
	select  
		gd.*,
		row_number() over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
	from game_details gd 
	join games g on g.game_id = gd.game_id 
),
test as(
select
	* 
from game_details_deduplicated
where row_num=1
)
select 
	game_id,
	team_id,
	player_id,
	count(*)
from test
group by 1,2,3
having  count(*)>1;
---------------------------------------------------------------------------------
--END OF TASK 1
---------------------------------------------------------------------------------


---------------------------------------------------
--TASK 2: A DDL for an user_devices_cumulated table
---------------------------------------------------

--CREATE user_devices_cumulated TABLE 
--truncate table user_devices_cumulated;
create table user_devices_cumulated (
	user_id text,
	device_id text,
	--the list of dates in the past which tracks a users active days by browser_type
	device_activity_datelist JSONB,
	--the current date of the device
	date date,
	primary key(user_id,device_id,date)
);
---------------------------------------------------
--END OF TASK 2
---------------------------------------------------


----------------------------------------------------------------------------
--TASK 3:A cumulative query to generate device_activity_datelist from events
----------------------------------------------------------------------------

--QUERY TO POPULATE user_devices_cumulated
--NOTE: Since we are working with data from the past, in order to populate the table day by day, the query needs to executed with the following logic:
--in the yesterday CTE you need to set the where filter manually to the previous day of data to retrive data from the user_devices_cumulated (it will retrive nulls if it is the first time you execute the wuery),
--then in the today CTE you need to set the were filter to de current day (that is the next day of the date you set in yersteday CTE). Once you configure that, you can execute the query with past data.
insert into user_devices_cumulated
with yesterday as( 
	select 
	*
	from user_devices_cumulated udc 
	where date = '2023-01-30'::date
),
today as (
select 
	cast(e.user_id as text) as user_id,
	cast(e.device_id as text) as device_id,
	d.browser_type as browser_type,
	cast(e.event_time as timestamp)::date as date_active
from events e 
join devices d on d.device_id = e.device_id 
where cast(e.event_time as timestamp)::date = '2023-01-31'::date 
and e.device_id is not null and e.user_id is not null
group by e.user_id,e.device_id,d.browser_type, cast(e.event_time as timestamp)::date
order by e.device_id
)
select
coalesce(t.user_id,y.user_id) as user_id,
coalesce(t.device_id,y.device_id) as device_id,
case 
	when y.device_activity_datelist is null 
		then jsonb_build_object(
				t.browser_type,jsonb_build_array(t.date_active)
			)
	when t.date_active is null
		then y.device_activity_datelist
	when y.device_activity_datelist is not null and y.device_activity_datelist::jsonb ? t.browser_type
		then jsonb_set(y.device_activity_datelist,array[t.browser_type],to_jsonb(t.date_active)||(y.device_activity_datelist->t.browser_type))
	else y.device_activity_datelist || jsonb_build_object(t.browser_type,jsonb_build_array(t.date_active))--jsonb_object(array[t.browser_type],array[t.date_active::TEXT])		
end as device_activity_datelist,
coalesce(t.date_active,y.date + interval '1 day') as date
from today t 
full outer join yesterday y
	on t.user_id=y.user_id and t.device_id=y.device_id
;
----------------------------------------------------------------------------
--END OF TASK 3
----------------------------------------------------------------------------


-----------------------------------------
--TASK 4: A datelist_int generation query
-----------------------------------------

--QUERY THAT CONVERTS device_activity_datelist COLUMN INTO A datelist_int COLUMN
--NOTE: the column datelist_int in the final select is a secuence of "1" and "0", where each 1 indicates an activity and 0 indicates there was no activity
with devices_activity as (
	select 
	*
	from user_devices_cumulated udc
	where udc."date" ='2023-01-31'::date
),
series as (
	select 
	*
	from generate_series('2023-01-01'::date,'2023-01-31'::date, interval '1 day') as series_date
),
dates_active as(
select 
da.user_id as user_id,
da.device_id as device_id,
array_agg(arr.value::date) as dates_active
from devices_activity da 
cross join lateral jsonb_each(device_activity_datelist) as kv(key,val)
cross join lateral jsonb_array_elements_text(val) as arr(value)
group by da.user_id,da.device_id,da.device_activity_datelist,da.date
),
datelist_ints as(
select 
case
	when da2.dates_active @> array[s.series_date::date]
		then cast(pow(2,32 - (da.date - s.series_date::date)) as bigint)
	else 0
end as placeholder_int_value,
da.user_id,
da.device_id,
da.device_activity_datelist,
da.date,
da2.dates_active, 
s.series_date
from devices_activity da
join dates_active da2 
	on da2.user_id=da.user_id and da2.device_id=da.device_id
cross join series s
)
select 
di.user_id,
di.device_id,
di.device_activity_datelist,
cast(cast(sum(di.placeholder_int_value) as bigint) as bit(32)) as datelist_int
from datelist_ints di
group by
	di.user_id,
	di.device_id,
	di.device_activity_datelist
;
-----------------------------------------
--END OF TASK 4
-----------------------------------------


-----------------------------------------
--TASK 5: A DDL for hosts_cumulated table
-----------------------------------------

--CREATE TABLE hosts_cumulated
--drop table hosts_cumulated;
create table hosts_cumulated (
	host text,
	host_activity_datelist date[],
	date date,
	primary key (host,date)
);
-----------------------------------------
--END OF TASK 5
-----------------------------------------

----------------------------------------------------------------
--TASK 6:he incremental query to generate host_activity_datelist
----------------------------------------------------------------

--QUERY TO POPULATE hosts_cumulated
--NOTE: Since we are working with data from the past, in order to populate the table day by day, the query needs to executed with the following logic:
--in the yesterday CTE you need to set the where filter manually to the previous day of data to retrive data from the user_devices_cumulated (it will retrive nulls if it is the first time you execute the wuery),
--then in the today CTE you need to set the were filter to de current day (that is the next day of the date you set in yersteday CTE). Once you configure that, you can execute the query with past data.
insert into hosts_cumulated
with yesterday as(
	select 
	* 
	from hosts_cumulated hc
	where hc.date ='2023-01-09'::date
),
today as( 
	select 
	e.host as host,
	cast(e.event_time as timestamp)::date as date_active
	from events e
	where cast(e.event_time as timestamp)::date='2023-01-10'::date
	group by e.host,cast(e.event_time as timestamp)::date
)
select 
coalesce(t.host,y.host) as host,
case 
	when y.host_activity_datelist is null then array[t.date_active]
	when t.date_active is null then y.host_activity_datelist
	else array[t.date_active] || y.host_activity_datelist 
end  as host_activity_datelist,
coalesce(t.date_active,y.date + interval '1 day') as date
from today t 
full outer join yesterday y on y.host=t.host
;
----------------------------------------------------------------
--END OF TASK 6
----------------------------------------------------------------

-----------------------------------------------------------------
--TASK 7: A monthly, reduced fact table DDL host_activity_reduced
-----------------------------------------------------------------

--CREATE TABLE host_activity_reduce
create table host_activity_reduce (
	month date,
	host text,
	hit_array real[],
	unique_visitors_array real[],
	primary key(month,host)
);
-----------------------------------------------------------------
--END OF TASK 7
-----------------------------------------------------------------

---------------------------------------------------------------
--TASK 8: An incremental query that loads host_activity_reduced
---------------------------------------------------------------

--QUERY TO POPULATE host_activity_reduce day by day
--NOTE: since we are wotking with past data, before the execution of the query you need to set the date in the were filter from the daily_aggregate CTE
insert into host_activity_reduce
with daily_aggregate as(
	select 
	e.host,
	cast(e.event_time as timestamp)::date as date,
	count(1) num_site_hits,
	count(distinct e.user_id) as unique_visitors_count
	from events e
	where cast(e.event_time as timestamp)::date='2023-01-10'::date
		and e.user_id is not null
	group by e.host,cast(e.event_time as timestamp)::date	
),
yesterday_array as ( 
	select 
	*
	from host_activity_reduce
	where month='2023-01-01'::date
)
select
coalesce(ya."month",date_trunc('month',da.date))::date as month,
coalesce(da.host,ya.host) as host,
case
	when ya.hit_array is not null
		then ya.hit_array || array[coalesce(da.num_site_hits,0)]
	when ya."month" is null
		then array_fill(0,array[da.date-date_trunc('month',da.date)::date])||array[coalesce(da.num_site_hits,0)]
end as hit_array,
case
	when ya.unique_visitors_array is not null
		then ya.unique_visitors_array || array[coalesce(da.unique_visitors_count,0)]
	when ya."month" is null
		then array_fill(0,array[da.date-date_trunc('month',da.date)::date])||array[coalesce(da.unique_visitors_count,0)]
end as unique_visitors_array
from daily_aggregate da
full outer join yesterday_array ya
	on da.host=ya.host
on conflict (month,host)
	do update set hit_array=excluded.hit_array, unique_visitors_array=excluded.unique_visitors_array
;

--NOTE: This las query display a table that summurized all the the number of hits and number of unique  visitiros, and present it day by day.
with agg as(
select 
month,
array[sum(hit_array[1]),
	  sum(hit_array[2]),
	  sum(hit_array[3]),
	  sum(hit_array[4]),
	  sum(hit_array[5]),
	  sum(hit_array[6]),
	  sum(hit_array[7]),
	  sum(hit_array[8]),
	  sum(hit_array[9]),
	  sum(hit_array[10])
	] as summed_array_hit_site,
array[sum(unique_visitors_array[1]),
	  sum(unique_visitors_array[2]),
	  sum(unique_visitors_array[3]),
	  sum(unique_visitors_array[4]),
	  sum(unique_visitors_array[5]),
	  sum(unique_visitors_array[6]),
	  sum(unique_visitors_array[7]),
	  sum(unique_visitors_array[8]),
	  sum(unique_visitors_array[9]),
	  sum(unique_visitors_array[10])
	] as summed_array_unique_visitor
from host_activity_reduce
group by  month
),
hit_agg as(
select
month as month_start,
month + cast(cast(a.index-1 as text)||' day' as interval) as day_of_month,
a.elem as num_site_hits
from agg 
cross join lateral unnest(agg.summed_array_hit_site) with ordinality as a(elem,index)
order by 2
),
visitor_agg as(
select
month as month_start,
month + cast(cast(b.index-1 as text)||' day' as interval) as day_of_month,
b.elem as num_unique_visitors
from agg 
cross join lateral unnest(agg.summed_array_unique_visitor) with ordinality as b(elem,index)
)
select 
ha.month_start,
ha.day_of_month,
ha.num_site_hits,
va.num_unique_visitors
from hit_agg ha
join visitor_agg va on va.day_of_month=ha.day_of_month
;

---------------------------------------------------------------
--END OF TASK 8
---------------------------------------------------------------