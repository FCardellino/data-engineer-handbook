--TASK 1: A query that does state change tracking for players

--CREATE THE TABLE THAT WILL TRACK THE CHANGES IN THE STATE OF EACH PLAYER
--truncate table players_state_tracker;
create table players_state_tracker (
	player_name text,
	first_active_season integer,
	last_active_season integer,
	season_state text,
	seasons_active integer[],
	season integer,
	primary key(player_name,season)
);


--QUERY TO POPULATE players_state_tracker TABLE SEASON BY SEASON
insert into players_state_tracker
with last_season as (
	select * from players_state_tracker
	where season=2000
),
this_season as( 
	select 
		player_name,
		draft_year::integer as draft_year,
		season
	from player_seasons
	where draft_year<>'Undrafted' and season=2001
)
select 
coalesce(ts.player_name,ls.player_name) as player_name,
coalesce(ls.first_active_season,ts.draft_year) as first_active_season,
coalesce(ts.season,ls.last_active_season) as last_active_season,
case
	when ls.player_name is null and ts.draft_year = ts.season then 'New' 
	when ls.player_name is null and ts.draft_year <> ts.season then 'Continued Playing'
	when ts.player_name is not null and ls.season_state in ('New','Continued Playing','Returned from Retirement') then 'Continued Playing'
	when ts.player_name is null and ls.season_state in ('New','Continued Playing','Returned from Retirement') then 'Retired'
	when ts.player_name is null and ls.season_state in ('Retired','Stayed Retired') then 'Stayed Retired'
	when ts.player_name is not null and ls.season_state in ('Retired','Stayed Retired') then 'Returned from Retirement'
end as season_state,
coalesce(ls.seasons_active,array[]::integer[])
		|| case 
			when ts.player_name is not null 
			 then array[ts.season]
			else array[]::integer[]
		end as	seasons_active,
coalesce(ts.season, ls.season+1) as season
from this_season ts  
full outer join last_season ls on ts.player_name=ls.player_name
;
--END OF TASK 1


--TASK 2:A query that uses GROUPING SETS to do efficient aggregations of game_details data

--CREATE A TABLE THAT COMBINES games AND game_details TABLE AND AGGREGATES THE DATA ALONG DIFERENT SETS OF DIMENSION GROUPS
--drop table agg_game_details;
create table agg_game_details as (
with combined as(
	select 
	gt.player_name,
	gt.team_id,
	g.season,
	gt.pts,
	g.home_team_wins as wins
	from 
	games g 
	join game_details gt on gt.game_id = g.game_id 
)
select 
	coalesce(player_name,'(overall)') as player_name,
	coalesce(team_id::text,'(overall)') as team_id,
	coalesce(season::text,'(overall)') as season,
	sum(pts) as total_pts,
	sum(wins) as total_wins
from combined
--AGGREGATE THE POINTS ALONG THE FOLLOWING GROUPING SETS
group by grouping sets(
	(player_name,team_id),
	(player_name,season),
	(team_id)
)
)
;

--Q1:who scored the most points playing for one team?
select player_name,total_pts from agg_game_details
where player_name<>'(overall)' and team_id<>'(overall)' and total_pts is not null 
order by total_pts desc
;
--Q2:who scored the most points in one season?
select player_name,season,total_pts from agg_game_details
where team_id='(overall)' and total_pts is not null 
order by total_pts desc
;
--Q3:which team has won the most games?
select team_id,total_wins from agg_game_details
where team_id<>'(overall)' and total_wins is not null 
order by total_wins desc
;
--END OF TASK 2


--TASK 3:A query that uses window functions on game_details to find out the following things:
--Q1:What is the most games a team has won in a 90 game stretch?

--CREATE A CTE THAT COMBINES game_details AND games TO RETRIVE THE NECESSARY DATA TO ANSWER THE QUESTION Q1
with combined as(
	select 
	g.game_date_est::date as game_date,
	gd.game_id,
	gd.team_id, 
	g.home_team_wins as game_result
	from game_details gd 
	left join games g on gd.game_id = g.game_id	and gd.team_id = g.home_team_id 
),
--CREATE A SECOND CTE TO DEDUPLICATE DATA
deduplication as (
	select
	game_date,
	game_id,
	team_id,
	game_result
	from combined 
	where game_date is not null
	group by game_date,
	game_id,
	team_id,
	game_result
),
--USE A WINDOW FUNTION TO NUMBER THE GAMES EACH TEAM PLAYS.
ordered_games as(
select *, row_number() over(partition by team_id order by game_date ) as game_number
from deduplication
),
--USE A WINDOW FUNCTION TO CALCULATE THE NUMBER OF WINS IN A 90 GAME STRETCH
rolling_wins as(
select
	game_date,
	game_id,
	team_id,
	sum(game_result) over(partition by team_id order by game_number rows between 89 preceding and current row) as wins_in_90_games
from ordered_games
)
select  
team_id,
max(wins_in_90_games) as max_wins_in_90_games_stretch
from rolling_wins
group by team_id
order by max_wins_in_90_games_stretch
;

--Q2:How many games in a row did LeBron James score over 10 points a game?
--CREATEA A CTE TO RETRIVE ONLY INFORMATION FOR LEBRON JAMES
with lebron_data as (
select 
gd.game_id,
gd.player_id,
gd.player_name,
gd.pts,
--CHECK FOR GAMES WHERE POINTS CONVERTED ARE OVER 10
case when pts>10 then 1 else 0 end as over_10,
--COUNT ONLY THOSE GAMES WHERE POINTS CONVERTED ARE LOWER OR EQUAL TO 10.
sum(case when pts<=10 then 1 else 0 end) over(order by game_id rows unbounded preceding) as grp
from game_details gd
where player_name = 'LeBron James'
order by sum(case when pts<=10 then 1 else 0 end) over(order by game_id rows unbounded preceding) desc
),
--CREATEA A CTE TO COUNT THOSE GAMES IN A ROW WHERE THE POINTS CONVERTED WERE OVER 10
aggregated as(
	select grp, count(*) as num_games
	from lebron_data
	where over_10=1
	group by grp
)
select 'LeBron James' as player, max(num_games) as max_num_games_over10pts_in_row
from aggregated
;
