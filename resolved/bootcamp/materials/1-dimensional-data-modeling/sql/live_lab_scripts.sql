select * from player_seasons ps; 

create type season_stats as (
	season integer,
	gp integer,
	pts real,
	reb real,
	ast real 
);

create table players(
	player_name text,
	height text,
	college text,
	country text,
	draft_year text,
	draft_around text,
	draft_number text,
	season_stats season_stats[],
	current_season integer,
	primary key (player_name, current_season)
);

select min(season) from player_seasons ps ;



insert into players 
with yesterday as( 
	select * from players p 
	where p.current_season = 2000
)
,
today as(
	select * from player_seasons ps 
	where ps.season = 2001
)
select
coalesce(t.player_name, y.player_name) as player_name,
coalesce(t.height, y.height) as height,
coalesce(t.college, y.college) as college,
coalesce(t.country, y.country) as country,
coalesce(t.draft_year, y.draft_year) as draft_year,
coalesce(t.draft_round, y.draft_around) as draft_around,
coalesce(t.draft_number, y.draft_number) as draft_number,
case 
	when y.season_stats is null 
		then array[row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
			)::season_stats]
	when t.season is not null then y.season_stats || array[row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
			)::season_stats]
	else y.season_stats
end as season_stats,
coalesce(t.season, y.current_season+1) as current_season
from today t full outer join yesterday y 
	on t.player_name = y.player_name ;


with unnested as(
select 
player_name,
unnest(season_stats)/*::season_stats*/ as season_stats
from players p where p.current_season = 2001
--and player_name='Michael Jordan'
)
select player_name,
(season_stats::season_stats).*
from unnested;




drop table players;

create type scoring_class as enum('star','good','average','bad');

create table players(
	player_name text,
	height text,
	college text,
	country text,
	draft_year text,
	draft_around text,
	draft_number text,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season integer,
	current_season integer,
	primary key (player_name, current_season)
);


insert into players 
with yesterday as( 
	select * from players p 
	where p.current_season = 2000
)
,
today as(
	select * from player_seasons ps 
	where ps.season = 2001
)
select
coalesce(t.player_name, y.player_name) as player_name,
coalesce(t.height, y.height) as height,
coalesce(t.college, y.college) as college,
coalesce(t.country, y.country) as country,
coalesce(t.draft_year, y.draft_year) as draft_year,
coalesce(t.draft_round, y.draft_around) as draft_around,
coalesce(t.draft_number, y.draft_number) as draft_number,
case 
	when y.season_stats is null 
		then array[row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
			)::season_stats]
	when t.season is not null then y.season_stats || array[row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
			)::season_stats]
	else y.season_stats
end as season_stats,
case
	when t.season is not null 
		then 
			case
				when t.pts > 20 then 'star'
				when t.pts > 15 then 'good' 
				when t.pts > 10 then 'average'
				else 'bad'
			end::scoring_class
	else y.scoring_class
end as scoring_class,
case
	when t.season is not null 
		then 0
	else y.years_since_last_season+1
end as yeas_since_last_season,
coalesce(t.season, y.current_season+1) as current_season
from today t full outer join yesterday y 
	on t.player_name = y.player_name ;

select 
player_name,
(season_stats[cardinality(season_stats)]::season_stats).pts /
case 
	when (season_stats[1]::season_stats).pts = 0
		then 1 
	else (season_stats[1]::season_stats).pts
end
from players where current_season = 2001
order by 2 desc;

----------------------------------------------------------------------------
----------------------------------------------------------------------------

 

drop table players;
create table players(
	player_name text,
	height text,
	college text,
	country text,
	draft_year text,
	draft_round text,
	draft_number text,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season integer,
	current_season integer,
	is_active boolean,
	primary key (player_name, current_season)
);




INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name ;


select p.player_name ,p."scoring_class", p.is_active
from players p
where p.current_season = 2022; 


drop table players_scd;
create table players_scd ( 
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
	current_season integer,
	primary key (player_name, start_season, end_season)
);



insert into players_scd
with with_previous as ( 
select 
	p.player_name ,
	p."current_season", 
	scoring_class,
	p.is_active,
	lag(p."scoring_class",1 ) over (partition by player_name order by current_season) as previous_scoring_class,
	lag(p."is_active",1 ) over (partition by player_name order by current_season) as previous_is_active	
from players p
where p.current_season <=2021
),
with_indicators as(
select 
	*,
	case	
		when scoring_class <> previous_scoring_class then 1
		when is_active <> previous_is_active then 1
		else 0
	end as change_indicator
from with_previous
),
with_streaks as (
select 
	*,
	sum(change_indicator) over (partition by player_name order by current_season) as streak_identifier
from with_indicators 
)
select 
	player_name,
	scoring_class,
	--streak_identifier ,
	is_active,
	min(current_season) as start_season,
	max(current_season) as end_season,
	2021 as current_season
from with_streaks 
group by 
	player_name,
	streak_identifier ,
	is_active,
	scoring_class
order by player_name,streak_identifier
;

select * from players_scd;




create type scd_type as (
scoring_class scoring_class,
is_active boolean,
start_season integer,
end_season integer
);



with last_season_scd as (
	select * from players_scd ps 
	where current_season=2021
	and end_season = 2021
),
historical_scd as(
	select 
	ps.player_name ,
	ps."scoring_class" ,
	ps.is_active ,
	ps.start_season ,
	ps.end_season 
	from players_scd ps 
	where current_season=2021
	and end_season < 2021
),
this_season_data as(
	select * from players p
	where current_season = 2022
),
unnchanged_records as (
select 
	ts.player_name ,
	ts."scoring_class",
	ts.is_active,
	ls.start_season,
	ts.current_season as end_season
from  this_season_data ts
join last_season_scd ls 
	on ls.player_name= ts.player_name 
where ts."scoring_class" = ls."scoring_class" 
	and ts.is_active = ls.is_active 
),
changed_records as ( 
select 
	ts.player_name ,
	unnest(array[
		row(
			ls.scoring_class,
			ls.is_active,
			ls.start_season,
			ls.end_season
		)::scd_type,
		row(
			ts.scoring_class,
			ts.is_active,
			ts.current_season,
			ts.current_season
		)::scd_type	
	]) as records
from  this_season_data ts
left join last_season_scd ls 
	on ls.player_name= ts.player_name 
where (ts."scoring_class" <> ls."scoring_class" 
	or ts.is_active <> ls.is_active)
),
unnested_change_records as(
select 
player_name,
(records::scd_type).scoring_class,
(records::scd_type).is_active,
(records::scd_type).start_season,
(records::scd_type).end_season
from changed_records
),
new_records as (
select
ts.player_name,
ts.scoring_class,
ts.is_active,
ts.current_season as star_season,
ts.current_season as end_season
from this_season_data ts
left join last_season_scd ls
	on ts.player_name=ls.player_name
where ls.player_name is null
)
select * from historical_scd
union all
select * from unnchanged_records
union all
select * from unnested_change_records
union all
select * from new_records 
;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
create type vertex_type as enum ('player','team', 'game');


create table vertices(
	identifier text,
	type vertex_type,
	properties json,
	primary  key (identifier, type)
);

create type edge_type as enum ('plays_against','shares_team','plays_in','plays_on');

create table edges (
	subject_identifier text,
	subject_type vertex_type,
	object_identifier text,
	object_type vertex_type,
	edge_type edge_type,
	properties json,
	primary key(subject_identifier,subject_type,object_identifier,object_type,edge_type)
);


insert into vertices 
select
game_id as identifier,
'game'::vertex_type as type,
json_build_object(
	'pts_home', pts_home,
	'pts_away', pts_away,
	'winning_team', case when  home_team_wins = 1 then home_team_id else visitor_team_id end
) as properties
from games;


insert into vertices
with players_agg as (
select 
player_id as identifier,
max(player_name) as player_name,
count(1) as number_of_games,
sum(pts) as total_points,
array_agg(distinct team_id) as teams
from game_details gd 
group by player_id)
select 
identifier,
'player'::vertex_type as type,
json_build_object(
	'player_name', player_name,
	'number_of_games', number_of_games,
	'total_points', total_points,
	'teams', teams
) as properties
from players_agg
;


insert into vertices
with teams_deduped as (
select 
*,
row_number() over(partition by team_id) as row_num
from teams
) 
select 
team_id as identifier,
'team'::vertex_type as type,
json_build_object(
	'abbreviation', abbreviation,
	'nickname', nickname,
	'city', city,
	'arena', arena,
	'year_founded', yearfounded 
) as properties
from teams_deduped
where row_num=1;

 
insert into edges
with deduped as ( 
select 
*, row_number() over(partition by player_id,game_id) as row_num
from game_details 
)
select 
player_id as subject_identifier,
'player'::vertex_type as subject_type,
game_id as object_identifier,
'game'::vertex_type as object_type,
'plays_in' as edge_type,
json_build_object(
	'start_position', start_position ,
	'pts', pts,
	'team_id', team_id,
	'team_abbreviation', team_abbreviation
) as properties
from deduped
where row_num=1;

select
v.properties->>'player_name',
max(cast(e.properties->>'pts' as integer))
from vertices v
join edges e
	on e.subject_identifier = v.identifier 
	and e.subject_type = v."type"
group by 1
order by 2 desc;

--Dimensional Data Modeling: Graph Data Modeling Day 3 Lab -> vi hasta -20:29


insert into edges
with deduped as ( 
select 
*, row_number() over(partition by player_id,game_id) as row_num
from game_details 
),
filtered as (
select 
* 
from deduped 
where row_num=1
),
aggregated as(
select
f1.player_id as subject_player_id, 
f2.player_id object_player_id,
case 
	when f1.team_abbreviation = f2.team_abbreviation
		then 'shares_team'::edge_type
	else 'plays_against'::edge_type
end as edge_type,
max(f1.player_name) as subject_player_name,
max(f2.player_name) as object_player_name,
count(1) as num_games,
sum(f1.pts) as subject_points,
sum(f2.pts) as object_points
from filtered f1
join filtered f2
	on f1.game_id =f2.game_id 
	and f1.player_name <> f2.player_name 
where f1.player_id > f2.player_id 
group by 
f1.player_id, 
f2.player_id,
case 
	when f1.team_abbreviation = f2.team_abbreviation
		then 'shares_team'::edge_type
	else 'plays_against'::edge_type
end
)
select 
subject_player_id as subject_identifier,
'player'::vertex_type as subject_type,
object_player_id as object_type,
'player'::vertex_type as object_type,
edge_type as edge_type,
json_build_object(
	'num_games', num_games,
	'subject_points', subject_points,
	'object_points', object_points
) as properties
from aggregated
;

select 
v.properties->>'player_name',
e.object_identifier,
cast(v.properties->>'number_of_games' as real)
/
case when cast(v.properties->>'total_points' as real)=0 then 1 else cast(v.properties->>'total_points' as real) end,
e.properties->>'num_games',
e.properties->>'subject_points'
from vertices v 
join edges e 
	on v.identifier = e.subject_identifier 
	and v."type" = e.subject_type 
where e.object_type = 'player'::vertex_type;












