from chispa.dataframe_comparer import *
from ..jobs.actors_history_scd import do_actors_history_scd_transformation
from collections import namedtuple

actorsData=namedtuple("actorsData","actor_id actor_name current_year quality_class is_active")
actorScd=namedtuple("actorsScd","actor_id actor_name quality_class is_active start_date end_date")

def test_actos_scd_generation(spark):
    source_data=[
        actorsData("nm0000001","Fred Astaire",1974,"average",True),
        actorsData("nm0000001","Fred Astaire",1975,"average",False),
        actorsData("nm0000001","Fred Astaire",1976,"bad",True),
        actorsData("nm0000001","Fred Astaire",1977,"average",True)
    ]
    source_df=spark.createDataFrame(source_data)

    actual_df=do_actors_history_scd_transformation(spark,source_df)
    
    expected_data=[
        actorScd("nm0000001","Fred Astaire","average",True,1974,1974),
        actorScd("nm0000001","Fred Astaire","average",False,1975,1975),
        actorScd("nm0000001","Fred Astaire","bad",True,1976,1976),
        actorScd("nm0000001","Fred Astaire","average",True,1977,1977),
    ]
    expected_df=spark.createDataFrame(expected_data)
    assert_df_equality(actual_df,expected_df,ignore_nullable=True,ignore_metadata=True)