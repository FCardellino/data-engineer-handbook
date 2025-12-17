--CHECK MAX/MIN YEAR IN DATASET TO CREATE SERIES
select max(year) from actor_films af;--2021
select min(year) from actor_films af;--1970

------------------------------
--TASK 1: DDL for actors table
-------------------------------

--CREATE FILMS TYPE TO BE USE IN ACTORS TABLE
--drop type films;
create type films as (
	year integer,
	film text,
	votes integer,
	rating float,
	filmid text
);

--CREATE QUALITY_CLASS TYPE TO BE USE IN ACTORS TABLE
--drop type quiality_class;
create type quality_class as enum (
	'star',
	'good',
	'average',
	'bad'
);

--CREATE ACTORS TABLE
--truncate table actors; 
create table actors (
	actor_id text,
	actor_name text,
	films films[],
	quality_class quality_class,
	--years_since_last_active integer,
	current_year integer,
	is_active boolean,
	primary key(actor_id,current_year)
);
------------------------------
--END OF TASK 1
-------------------------------


-------------------------------------------
--TASK 2: Cumulative table generation query
-------------------------------------------

--QUERY TO POPULATES ACTORS TABLE
insert into actors
with years as(
	select *
	from generate_series(1970,2021) as performance_year --I CHECK THE MAX AND MIN YEAR VALUE FROM ACTORS_FILM TABLE, TO CREATE THIS SERIE
),
a as ( 
	select 
	af.actorid as actor_id,
	af.actor as actor_name,
	min(af.year) as first_performance
	from actor_films af 
	group by 
	af.actorid,
	af.actor
),
actors_and_performances as(
	select *
	from a 
	join years y 
		on a.first_performance <= y.performance_year
),
windowed as(
select distinct
 aap.actor_id ,
 aap.actor_name,
 aap.performance_year,
 array_remove(array_agg(
 	case
 		when af.year is not null 
 			then row(
 				af.year,
 				af.film,
 				af.votes,
 				af.rating,
 				af.filmid 
 			)::films
 	end) 
 	over (partition by aap.actor_id order by coalesce(aap.performance_year,af.year) ),
 	null)
 	as films
from actors_and_performances aap
left join actor_films af
	on aap.actor_id = af.actorid 
	and aap.performance_year=af."year"
/*group by 
	aap.actor_id ,
 	aap.actor_name,
 	aap.performance_year*/
order by aap.actor_id, aap.performance_year),
static as(
	select 
	actorid as actor_id,
	actor as actor_name,
	max(year) as last_year_active
	from actor_films
	group by actorid,actor
)
select 
w.actor_id,
w.actor_name,
w.films as films,
case
	when (films[cardinality(films)]::films).rating > 8 then 'star'
	when (films[cardinality(films)]::films).rating > 7 and (films[cardinality(films)]::films).rating <=8  then 'good'
	when (films[cardinality(films)]::films).rating > 6 and (films[cardinality(films)]::films).rating <=7  then 'average'
	when (films[cardinality(films)]::films).rating <=6  then 'bad'
end::quality_class as quiality_class,
--w.performance_year - (films[CARDINALITY(films)]::films).year as years_since_last_active,
w.performance_year as year,
(films[CARDINALITY(films)]::films).year = w.performance_year AS is_active
from
windowed w 
join static s
	on w.actor_id=s.actor_id
--where w.actor_id='nm0000002' --and w.performance_year=1988 
order by w.performance_year
;
-------------------------------------------
--END OF TASK 2
-------------------------------------------


-------------------------------------------
--TASK 3: DDL for actors_history_scd table
-------------------------------------------

--CREATE ACTORS_HISTORY_SCD TABLE
--drop table actors_history_scd
create table actors_history_scd ( 
	actor_id text,
	actor_name text,
	quality_calss quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	current_year integer,
	primary key(actor_id,start_date,end_date)
);
-------------------------------------------
--END OF TASK 3
-------------------------------------------


-----------------------------------------------
--TASK 4: Backfill query for actors_history_scd
-----------------------------------------------

--BACKFILL QUERY TO POPULATE THE ENTIRE ACTORS_SCD_TABLE
--NOTE: THE QUERY ONLY INSERT DATA UP TO 2020, SO THE DATA OF 2021 CAN BE USED IN THE NEXT TASK
insert into actors_history_scd
with with_previous as ( 
	select 
	a.actor_id ,
	a.actor_name,
	a.current_year,
	a.quality_class,
	a.is_active,
	lag(a.quality_class) over (partition by a.actor_id order by a.current_year) as previous_quality_class,
	lag(a.is_active) over (partition by a.actor_id order by a.current_year) as previous_is_active
	from actors a 
	where a.current_year <=2020
),
with_indicators as(
	select 
		*,
		case
			when quality_class<>previous_quality_class then 1
			when is_active<>previous_is_active then 1
			else 0
		end as change_indicator
	from with_previous
),
with_streaks as (
	select
		*,
		sum(change_indicator) over  (partition by actor_id order by current_year) as streak_identifier
	from with_indicators
)
	select 
		actor_id ,
		actor_name ,
		quality_class ,
		is_active ,
		min(current_year) as start_date,
		max(current_year) as end_date,
		2020 as current_year
	from with_streaks 
	group by  
		actor_id ,
		actor_name ,
		streak_identifier,
		quality_class ,
		is_active
	order by actor_id, streak_identifier
; 
-----------------------------------------------
--EN OF TASK 4
-----------------------------------------------




--------------------------------------------------
--TASK 5: Incremental query for actors_history_scd
--------------------------------------------------

--CREATE ACTORS_SCD_TYPE TYPE TO BE USED IN THE INCREMENTAL QUERY
create type actors_scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer
);

--INCREMENTAL QUERY THAT COMBINES THE PREVIOUS YEARS OF SCD DATA WITH NEW DATA FROM ACTORS TABLE (DATA OF 2021 EXCLUDED FROM THE PREVIOUS INGESTION OF DATA - TASK 4)
--insert into actors_history_scd
with last_year_data_scd as ( 
	select 
	*
	from actors_history_scd
	where current_year=2020
	and end_date=2020
),
historical_scd as (
	select
		ahs.actor_id ,
		ahs.actor_name ,
		ahs.quality_calss ,
		ahs.is_active, 
		ahs.start_date ,
		ahs.end_date
	from actors_history_scd ahs
	where current_year=2020
	and end_date<2020
),
this_year_data as (
	select
	*
	from actors 
	where current_year=2021
),
unchanged_records as ( 
	select 
	ty.actor_id ,
	ty.actor_name ,
	ty."quality_class" ,
	ty.is_active ,
	ly.start_date ,
	ty.current_year as end_date
	from this_year_data ty
	join last_year_data_scd ly
		on ty.actor_id = ly.actor_id 
	where ty."quality_class" = ly.quality_calss 
	and ty.is_active =ly.is_active 
),
changed_records as (
select 
	ty.actor_id, 
	ty.actor_name,
	unnest(array[
		row(
			ly.quality_calss, 
			ly.is_active,
			ly.start_date,
			ly.end_date
		)::actors_scd_type,
		row(
			ty.quality_class, 
			ty.is_active,
			ty.current_year,
			ty.current_year
		)::actors_scd_type
	]) as records
from this_year_data ty
join last_year_data_scd ly
	on ty.actor_id = ly.actor_id 
where ty."quality_class" <> ly.quality_calss 
or ty.is_active <> ly.is_active 
),
unnested_changed_records as (
select 
	actor_id,
	actor_name,
	(records::actors_scd_type).quality_class,
	(records::actors_scd_type).is_active,
	(records::actors_scd_type).start_date,
	(records::actors_scd_type).end_date
from changed_records
),
new_records as(
select 
ty.actor_id,
ty.actor_name,
ty.quality_class,
ty.is_active,
ty.current_year as start_date,
ty.current_year as end_date
from this_year_data ty
left join last_year_data_scd ly
	on ty.actor_id=ly.actor_id
where ly.actor_id is null
)
select 
*,
2021 as current_year
from 
(
	select * from historical_scd 
	union all
	select * from unchanged_records
	union all 
	select * from unnested_changed_records
	union all
	select * from new_records
) a
order by actor_name,start_date
;
--------------------------------------------------
--END OF TASK 5
--------------------------------------------------
