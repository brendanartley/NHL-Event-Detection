from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import re
import sys
import json
import string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql.functions import col, max as max_, min as min_
#from pyspark.sql.types import StringType


def main(input_gamestats, input_eventdetection , output):

    gamestats_schema = types.StructType([
    types.StructField('game_id', types.StringType()),
    types.StructField('event_datetime_minute', types.TimestampType()),
    types.StructField('count', types.IntegerType()),
    types.StructField('is_scoring_event', types.StringType()),
    types.StructField('is_penalty_event', types.StringType()),
    types.StructField('event', types.StringType())
     ])

    eventdetection_schema = types.StructType([
    types.StructField('game_id', types.StringType()),
    types.StructField('event_datetime_minute', types.TimestampType()),
    types.StructField('count', types.IntegerType()),
    types.StructField('event', types.StringType())
     ])

    gamestates_df = spark.read.json(input_gamestats, schema=gamestats_schema)
    gamestates_df = gamestates_df.withColumnRenamed('game_id','game_id_gs').withColumnRenamed('event_datetime_minute','event_datetime_minute_gs').withColumnRenamed('count','count_gs').withColumnRenamed('event','event_ground_truth')
    
    event_detection_df = spark.read.json(input_eventdetection, schema=eventdetection_schema)
    event_detection_df = event_detection_df.withColumnRenamed('event','event_detected').withColumnRenamed('count','activity_count')

    timeline_combined_df = event_detection_df.join(gamestates_df, (event_detection_df['game_id'] == gamestates_df['game_id_gs']) & (event_detection_df['event_datetime_minute'] == gamestates_df['event_datetime_minute_gs']), 'inner' )
    final_output_df = timeline_combined_df.select(timeline_combined_df['game_id'], timeline_combined_df['event_datetime_minute'], timeline_combined_df['activity_count'],timeline_combined_df['event_detected'],timeline_combined_df['event_ground_truth'])

    final_output_df = final_output_df.orderBy('game_id', 'event_datetime_minute')
    # coalescing all the events for visualization on PowerBI
    final_output_df.coalesce(1).write.json(output)

    # spark-submit visualization_combiner.py gamestats_output_json visualizaiton_output output_combined
    # rm -r output_combined



if __name__ == '__main__':
    input_gamestats = sys.argv[1]
    input_eventdetection = sys.argv[2]
    output = sys.argv[3]

    spark = SparkSession.builder.appName('visualization_combiner').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input_gamestats, input_eventdetection , output)