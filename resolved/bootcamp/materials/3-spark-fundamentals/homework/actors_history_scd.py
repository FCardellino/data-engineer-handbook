from pyspark.sql import SparkSession

query="""
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
		max(current_year) as end_date
	from with_streaks 
	group by  
		actor_id ,
		actor_name ,
		streak_identifier,
		quality_class ,
		is_active
	order by actor_id, streak_identifier
; 
"""

def do_actors_history_scd_transformation(spark,dataframe):
	dataframe.createOrReplaceTempView("actors")
	return spark.sql(query)

def main():
	spark=SparkSession.builder\
			.master("local")\
			.appName("actors_history_scd")\
			.getOrCreate()
	output_dataframe=do_actors_history_scd_transformation(spark,spark.table("actors"))
	output_dataframe.write.mode("overwrite").insertInto("actors_history_scd")