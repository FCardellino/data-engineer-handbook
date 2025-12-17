--drop table fct_game_details;
create table fct_game_details(
	dim_game_date date,
	dim_season integer,
	dim_team_id integer,
	dim_player_id integer,
	dim_player_name text,
	dim_start_position text,
	dim_is_playing_at_home boolean,
	dim_did_not_play boolean,
	dim_did_not_dress boolean,
	dim_not_with_team boolean,
	m_minutes real,
	m_fgm integer,
	m_fga integer, 
	m_fg3a integer,
	m_ftm integer,
	m_fta integer,
	m_oreb integer,
	m_dreb integer,
	m_reb integer,
	m_ast integer,
	m_stl integer,
	m_blk integer,
	m_turnovers integer,
	m_pf integer,
	m_pts integer,
	m_plus_minus integer,
	primary key(dim_game_date,dim_team_id,dim_player_id)
);



select 
	game_id,
	team_id,
	player_id,
	count(*)
from game_details
group by 1,2,3
having  count(*)>1;

insert into fct_game_details
with dedouped as( 
	select 
		g.game_date_est,
		g.season,
		g.home_team_id ,
		--g.visitor_team_id ,
		gd.*,
		row_number() over(partition by gd.game_id,team_id,player_id order by g.game_date_est) as row_num
	from game_details gd 
	join games g on gd.game_id = g.game_id 
)
select 
	game_date_est as dim_game_date,
	season as dim_season,
	--home_team_id,
	--visitor_team_id,
	team_id as dim_team_id,
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_start_position,
	team_id = home_team_id  as dim_is_playing_at_home,
	coalesce(position('DNP' in comment),0)>0 as dim_did_not_play,
	coalesce(position('DND' in comment),0)>0 as dim_did_not_dress,
	coalesce(position('NWT' in comment),0)>0 as dim_not_with_team,
	--comment,
	cast(split_part(min,':',1) as real)
	+
	cast(split_part(min,':',2) as real)/60 as m_minutes,
	fgm as m_fgm,
	fga as m_fga,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	pf as m_pf,
	pts as m_pts,
	plus_minus as m_plus_minus 
from dedouped
where row_num=1
;

select 
dim_player_name,
dim_is_playing_at_home,
count(1) as num_games,
sum(m_pts) as total_pints,
count(case when dim_not_with_team then 1 end) as balied_num,
cast(count(case when dim_not_with_team then 1 end) as real)/count(1) as bail_pct
from fct_game_details
group by 1,2
order by 6 desc;
---------------------------------------------------------------


select
max(event_time) 
from events e ;





drop table users_cumulated;
create table users_cumulated( 
	user_id text,
	--the list of dates in the past where the user was active
	dates_active date[],
	--the current date of the user
	date date,
	primary key (user_id, date)
);



insert into users_cumulated
with yesterday as(  
	select 
	*
	from users_cumulated
	where date = '2023-01-30'::date
),
today as(  
	select 
	cast(e.user_id as text) as user_id,
	cast(e.event_time as timestamp)::date as date_active
	from events e 
	where cast(e.event_time as timestamp)::date = '2023-01-31'::date
	and user_id is not null
	group by user_id,cast(e.event_time as timestamp)::date
)
select 
coalesce(t.user_id,y.user_id) as user_id,
case 
	when y.dates_active is null then array[t.date_active]
	when t.date_active is null then y.dates_active
	else array[t.date_active] || y.dates_active 
end  as dates_active,
coalesce(t.date_active,y.date + interval '1 day') as date
from today t 
full outer join yesterday y
	on t.user_id = y.user_id
;

select * from users_cumulated 
where date = '2023-01-25'::date;


with users as ( 
select * from users_cumulated 
where date = '2023-01-31'::date
),
series as( 
select 
*
from generate_series('2023-01-01'::date,'2023-01-31'::date, interval '1 day') as series_date
),
place_holder_ints as(
select 
case 
	when u.dates_active @> array[s.series_date::date]
		then cast(pow(2,32 - (u.date - s.series_date::date)) as bigint)
	else 0
end  as placeholder_int_value --allows to see how long ago a user was active. More to the right is the 1 it means the user was ative many days ago in a serie of 31 days
,
*
from users u
cross join series s
--where user_id='12659532009806200000';
)
select 
user_id,
cast(cast(sum(placeholder_int_value) as bigint ) as bit(32)) ,
bit_count(cast(cast(sum(placeholder_int_value) as bigint ) as bit(32))) > 0 as dim_is_monthly_active,
bit_count(cast('11111110000000000000000000000000' as bit(32)) &
cast(cast(sum(placeholder_int_value) as bigint ) as bit(32))) > 0 as dim_is_weekly_active,
bit_count(cast('10000000000000000000000000000000' as bit(32)) &
cast(cast(sum(placeholder_int_value) as bigint ) as bit(32))) > 0 as dim_is_daily_active
from place_holder_ints
group by user_id
;

-------------------------------------------------------------------------------



create table array_metrics ( 
	user_id numeric,
	month_start date,
	metric_name text,
	metric_array real[],
	primary key(user_id,month_start,metric_name)
);

insert into array_metrics
with daily_aggregate as( 
	select 
	user_id,
	event_time::date as date,
	count(1) as num_site_hits
	from events 
	where event_time::date='2023-01-04'::date
	and user_id is not null
	group by user_id, event_time::date
),
yesterday_array as ( 
	select
	* 
	from array_metrics 
	where month_start='2023-01-01'::date
)
select 
coalesce(da.user_id,ya.user_id) as user_id,
coalesce(ya.month_start,date_trunc('month',da.date)) as month_start,
'site_hits' as metric_name,
case 
	when ya.metric_array is not null 
		then ya.metric_array || array[coalesce(da.num_site_hits,0)]
	when ya.month_start is null 
		 then array_fill(0, array[da.date-date_trunc('month',da.date)::date]) || array[coalesce(da.num_site_hits,0)]	
	/*when ya.metric_array is null 
		then array_fill(0, array[coalesce(da.date-ya.month_start,0)]) || array[coalesce(da.num_site_hits,0)]	*/
end as metric_array
from
daily_aggregate da
full outer join yesterday_array ya 
	on da.user_id=ya.user_id
on conflict (user_id,month_start,metric_name)
do 
	update set metric_array=excluded.metric_array
;

select 
cardinality(metric_array),count(1)
from array_metrics 
--where user_id=3920471813881165300
group by 1
;

delete from array_metrics;

---------------------------------------------
with agg as(
select 
metric_name,
month_start,
array[sum(metric_array[1]),
	  sum(metric_array[2]),
	  sum(metric_array[3]),
	  sum(metric_array[4])
	] as summed_array
from array_metrics
group by metric_name, month_start
)
select 
metric_name,
month_start + cast(cast(index-1 as text)||' day' as interval) ,
elem as value
from agg 
cross join unnest(agg.summed_array) with ordinality as a(elem,index)
;



































