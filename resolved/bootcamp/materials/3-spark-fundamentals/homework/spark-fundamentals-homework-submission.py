from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, split, broadcast

#CREATE AND CONFIGURE SPARK SESSION
spark = SparkSession.builder.appName("SparkFundamentalsWeekHW")\
        .config("spark.executor.memory", "4g")\
        .config("spark.driver.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "200")\
        .config("spark.sql.files.maxPartitionBytes", "134217728")\
        .config("spark.dynamicAllocation.enabled", "true")\
        .config("spark.dynamicAllocation.minExecutors", "1")\
        .config("spark.dynamicAllocation.maxExecutors", "50")\
        .getOrCreate()
##TASK 1: Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

#Disabled automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
##END OF TASK 1

##TASK 2: Explicitly broadcast JOINs medals and maps
#SAVE DATA INTO VARIABLES 
medalsSelected = spark.read.option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/home/iceberg/data/medals.csv")
mapsSelected = spark.read.option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/home/iceberg/data/maps.csv")
matchesSelected = spark.read.option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/home/iceberg/data/matches.csv")
matchDetailsSelected = spark.read.option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/home/iceberg/data/match_details.csv")
medalsMatchesPlayersSelected = spark.read.option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/home/iceberg/data/medals_matches_players.csv")

#CREATE TEMP VIEWS FOR EXPLICIT BRODCAST JOIN
medalsSelected.createOrReplaceTempView("medals")
mapsSelected.createOrReplaceTempView("maps")
matchesSelected.createOrReplaceTempView("matches")
medalsMatchesPlayersSelected.createOrReplaceTempView("medals_matches_players")

#DO AND SAVE EXPLICIT BROADCAST INTO VARIABLEs
explicitBroadcastMedals = medalsMatchesPlayersSelected.alias("mmp")\
                                                          .join(broadcast(medalsSelected).alias("me"),col("mmp.medal_id") == col("me.medal_id"))\
                                                          .select("mmp.*","me.classification")
explicitBroadcastMaps = matchesSelected.alias("m")\
                                           .join(broadcast(mapsSelected).alias("ma"),col("m.mapid") == col("ma.mapid"))\
                                           .select("m.*","ma.name")

##END OF TASK 2

##TASK 3: Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets

#CREATE TABLES WITH BUCKETS AND SAVE BUCKETED DATA
#CREATE TABLE medals_matches_players_bucketed
spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")
bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
     match_id STRING,
     player_gamertag STRING,
     medal_id STRING,
     count INTEGER,
     classification STRING
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDDL)

#LOAD DATA INTO BUCKETED TABLE
explicitBroadcastMedals.select(
    "match_id", "player_gamertag", "medal_id", "count", "classification"
    )\
    .write.mode("append")\
    .bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_bucketed")

#CREATE TABLE matches_maps_bucketed
#Get distinct completion dates
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_maps_bucketed""")
bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_maps_bucketed (
     match_id STRING,
     mapid STRING,
     name STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDDL)

#LOAD DATA INTO BUCKETED TABLE
explicitBroadcastMaps.select(
    "match_id", "mapid", "name", "is_team_game", "playlist_id", "completion_date"
    )\
    .write.mode("append")\
    .partitionBy("completion_date")\
    .bucketBy(16, "match_id").saveAsTable("bootcamp.matches_maps_bucketed")


#CREATE TABLE match_details_bucketed
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDDL)

#LOAD DATA INTO BUCKETED TABLE
matchDetailsSelected.select(
    "match_id", "player_gamertag", "player_total_kills", "player_total_deaths")\
    .write.mode("append")\
    .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")


#JOIN TABLES TO CREATE DATAFRAME
spark.sql(
"""
    SELECT 
         mmb.match_id,
         mmb.mapid, 
         mmb.name as map_name, 
         mmb.is_team_game, 
         mmb.playlist_id, 
         mmb.completion_date,
         mdb.player_gamertag, 
         mdb.player_total_kills, 
         mdb.player_total_deaths,
         mmpb.medal_id, 
         mmpb.count as medals_count,
         mmpb.classification as medal_classification
    FROM bootcamp.matches_maps_bucketed AS mmb
    JOIN bootcamp.match_details_bucketed AS mdb ON mdb.match_id=mmb.match_id
    JOIN bootcamp.medals_matches_players_bucketed AS mmpb ON mmpb.match_id=mmb.match_id
"""
).createOrReplaceTempView("filteredDF")

##END OF TASK 3

##TASK 4: Aggregate the joined data frame to figure out the following questions

#Which player averages the most kills per game?
players_avg_kills = spark.sql(
"""
    WITH deduplication as(
    SELECT 
         match_id,
         player_gamertag, 
         player_total_kills
    FROM filteredDF
    GROUP BY match_id,player_gamertag,player_total_kills)
    SELECT 
        match_id,
        player_gamertag,
        sum(player_total_kills)/count(distinct match_id) as avg_kills_per_game
    FROM deduplication
    GROUP BY match_id,player_gamertag
    ORDER BY sum(player_total_kills)/count(distinct match_id) desc
"""
)
#Which playlist gets played the most?
playlists = spark.sql(
"""
    WITH playlist_played as(
    SELECT 
         match_id,
         playlist_id, 
         count(1) as playlist_count
    FROM filteredDF
    GROUP BY match_id,playlist_id)
    SELECT 
        playlist_id,
        sum(playlist_count) num_plays
    FROM playlist_played
    GROUP BY playlist_id
    ORDER BY sum(playlist_count) desc
"""
)
#Which map gets played the most?
maps_played = spark.sql(
"""
    WITH maps_played as(
    SELECT 
         match_id,
         mapid,
         map_name,
         count(1) as map_count
    FROM filteredDF
    GROUP BY match_id,mapid,map_name
    )
    SELECT 
        mapid,
        map_name,
        sum(map_count) num_plays
    FROM maps_played
    GROUP BY  mapid,map_name
    ORDER BY sum(map_count) desc
"""
)
#Which map do players get the most Killing Spree medals on?
most_killing_spree = spark.sql(
"""
    WITH maps_and_medals as(
    SELECT 
         mapid,
         map_name,
         medal_classification,
         medals_count
    FROM filteredDF
    WHERE medal_classification='KillingSpree'
    GROUP BY mapid,map_name,medal_classification,medals_count
    )
    SELECT 
         mapid,
         map_name,
         medal_classification,
         sum(medals_count) num_medals
    FROM maps_and_medals
    GROUP BY  mapid,map_name,medal_classification
    ORDER BY sum(medals_count) desc
"""
)
##END OF TASK 4

#TASK 5: Try different .sortWithinPartitions to see which has the smallest data size

#SORT AGGREGATED DATAFRAME
sorted_df=players_avg_kills.sortWithinPartitions(col("match_id"))

#CREATE SORTED TABLE
spark.sql("""DROP TABLE IF EXISTS bootcamp.players_avg_kills_sorted""")
sortedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.players_avg_kills_sorted (
    match_id STRING,
    player_gamertag STRING,
    avg_kills_per_game REAL
 )
 USING iceberg;
 """
spark.sql(sortedDDL)
#LOAD DATA INTO BUCKETED TABLE
sorted_df.select(
    "match_id", "player_gamertag", "avg_kills_per_game")\
    .write.mode("append")\
    .saveAsTable("bootcamp.players_avg_kills_sorted")

#SORT AGGREGATED DATAFRAME
sorted_df=playlists.sortWithinPartitions(col("playlist_id"))
#CREATE SORTED TABLE
spark.sql("""DROP TABLE IF EXISTS bootcamp.playlists_sorted""")
sortedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.playlists_sorted (
    playlist_id STRING,
    num_plays INTEGER
 )
 USING iceberg;
 """
spark.sql(sortedDDL)
#LOAD DATA INTO BUCKETED TABLE
sorted_df.select(
    "playlist_id", "num_plays")\
    .write.mode("append")\
    .saveAsTable("bootcamp.playlists_sorted")

#SORT AGGREGATED DATAFRAME
sorted_df=maps_played.sortWithinPartitions(col("mapid"))
#CREATE SORTED TABLE
spark.sql("""DROP TABLE IF EXISTS bootcamp.maps_played_sorted""")
sortedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.maps_played_sorted (
    mapid STRING,
    map_name STRING,
    num_plays INTEGER
 )
 USING iceberg;
 """
spark.sql(sortedDDL)
#LOAD DATA INTO BUCKETED TABLE
sorted_df.select(
    "mapid", "map_name", "num_plays")\
    .write.mode("append")\
    .saveAsTable("bootcamp.maps_played_sorted")

#SORT AGGREGATED DATAFRAME
sorted_df=most_killing_spree.sortWithinPartitions(col("mapid"))
#CREATE SORTED TABLE
spark.sql("""DROP TABLE IF EXISTS bootcamp.most_killing_spree_sorted""")
sortedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.most_killing_spree_sorted (
    mapid STRING,
    map_name STRING,
    medal_classification STRING,
    num_medals INTEGER
 )
 USING iceberg;
 """
spark.sql(sortedDDL)
#LOAD DATA INTO BUCKETED TABLE
sorted_df.select(
    "mapid", "map_name", "medal_classification", "num_medals")\
    .write.mode("append")\
    .saveAsTable("bootcamp.most_killing_spree_sorted")

#COMPARE SORTED DATAFRAME SIZES
spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'players_avg_kills_sorted' as DF  FROM bootcamp.players_avg_kills_sorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'playlists_sorted' as DF FROM bootcamp.playlists_sorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'maps_played_sorted' as DF FROM bootcamp.maps_played_sorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'most_killing_spree_sorted' as DF FROM bootcamp.most_killing_spree_sorted.files
""").show()
##END OF TASK 5