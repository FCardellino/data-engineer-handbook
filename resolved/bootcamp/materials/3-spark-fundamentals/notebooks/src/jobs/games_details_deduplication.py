from pyspark.sql import SparkSession

query="""
with game_details_deduplicated as(
	select  
		gd.game_id, gd.team_id, gd.player_id, g.game_date_est,
		row_number() over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
	from game_details gd 
	join games g on g.game_id = gd.game_id
)
select
	game_id, team_id, player_id, game_date_est, cast(row_num as bigint) row_num
from game_details_deduplicated
where row_num=1
;
"""

def do_games_detail_deduplication_transformation(spark,dataframe:list):
    dataframe[0].createOrReplaceTempView("game_details")
    dataframe[1].createOrReplaceTempView("games")
    return spark.sql(query)

def main():
    spark=SparkSession.builder\
            .master("local")\
            .appName("game_details_dedup")\
            .getOrCreate()
    output_dataframe=do_games_detail_deduplication_transformation(spark,[spark.table("game_details"),spark.table("games")])
    output_dataframe.write.mode("overwrite").insertInto("games_detail_preprocessed")