from chispa.dataframe_comparer import *
from ..jobs.games_details_deduplication import do_games_detail_deduplication_transformation
from collections import namedtuple

gamesDetailsData=namedtuple("gamesDetailsData","game_id team_id player_id")
gamesData=namedtuple("gamesData","game_id game_date_est")
gamesDetailsDedup=namedtuple("gamesDetailsDedup","game_id team_id player_id game_date_est row_num")

def test_games_details_deduplication(spark):
    games_details_sd=[
        gamesDetailsData(22000001,1610612744,201939),
        gamesDetailsData(22000001,1610612744,201939)
    ]
    gd_input_df=spark.createDataFrame(games_details_sd)
    
    games_sd=[
        gamesData(22000001,"2020-12-22")
    ]
    g_input_df=spark.createDataFrame(games_sd)

    actual_df=do_games_detail_deduplication_transformation(spark,[gd_input_df,g_input_df])

    expected_data=[
        gamesDetailsDedup(22000001,1610612744,201939,"2020-12-22",1)
    ]
    expected_df=spark.createDataFrame(expected_data)

    assert_df_equality(actual_df,expected_df,ignore_nullable=True)
