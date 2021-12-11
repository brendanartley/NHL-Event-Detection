from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import re
import sys
import json
import string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql.functions import col, max as max_, min as min_


def main(inputs, output):

    gamestats = spark.read.parquet(inputs)
    #gamestats.printSchema()
    
    # Reomve the events not directly relevant to the game
    gamestats = gamestats.where( (gamestats['event_name'] != functions.lit('Game Scheduled')) & (gamestats['event_name'] != functions.lit('Game Official')) )

    #gamestats_by_game = gamestats.where(gamestats['game_id'] == functions.lit('2020020001'))
    gamestats_by_game = gamestats
    gamestats_by_game = gamestats_by_game.withColumn('event_datetime_altered', functions.concat(functions.substring(gamestats_by_game['event_datetime'],1,16), functions.lit(':00Z') ) )
    gamestats_by_game = gamestats_by_game.withColumn('event_datetime_minute', functions.to_timestamp(gamestats_by_game['event_datetime_altered']))
    gamestats_by_game = gamestats_by_game.drop(gamestats_by_game['event_datetime_altered']).drop(gamestats_by_game['event_datetime']).cache()
    
    
    # Create a dataframe where each minute is present as a row from the game start time to the end
    complete_timeline_min_df = gamestats_by_game.groupBy('game_id').agg(min_("event_datetime_minute").alias('starttime'))
    complete_timeline_max_df = gamestats_by_game.groupBy('game_id').agg(max_("event_datetime_minute").alias('endtime'))
    complete_timeline_max_df = complete_timeline_max_df.withColumnRenamed('game_id','game_id_max')

    complete_timeline_df = complete_timeline_min_df.join(complete_timeline_max_df, complete_timeline_min_df['game_id'] == complete_timeline_max_df['game_id_max'])
    complete_timeline_df = complete_timeline_df.drop(complete_timeline_df['game_id_max'])
    
    complete_timeline_df = complete_timeline_df.select(complete_timeline_df['game_id'], functions.explode(functions.expr('sequence(starttime, endtime, interval 1 minute)')).alias('event_datetime_minute'))
    complete_timeline_df.createOrReplaceTempView('complete_timeline')


    # Gather signifivant events as a dataset
    event_collection_df = gamestats_by_game.where( (gamestats_by_game['is_scoring_event'] == True) | (gamestats_by_game['is_penalty_event'] == True) )
    event_collection_df = event_collection_df.select(event_collection_df['game_id'].alias('game_id_event'),event_collection_df['event_datetime_minute'].alias('event_datetime_event'), event_collection_df['is_scoring_event'] ,event_collection_df['is_penalty_event'] )

    # Group Number of events detected by minute
    event_count_df = gamestats_by_game.groupBy('game_id', 'event_datetime_minute').count()

    event_timeline_df = event_count_df.join(functions.broadcast(event_collection_df), (event_count_df['game_id'] == event_collection_df['game_id_event']) & (event_count_df['event_datetime_minute'] == event_collection_df['event_datetime_event']), 'leftouter')
    event_timeline_df = event_timeline_df.drop(event_timeline_df['game_id_event']).drop(event_timeline_df['event_datetime_event'])

    event_timeline_df.createOrReplaceTempView('event_timeline')

    # Combine the complete timeline with the event timeline
    final_output_df  = spark.sql("""
                        SELECT game_id, event_datetime_minute,
                               CASE WHEN count IS NULL THEN 0 ELSE count END AS count,
                               is_scoring_event, is_penalty_event
                        FROM (
                          SELECT ct.game_id, ct.event_datetime_minute,
                               et.count,
                               et.is_scoring_event,
                               et.is_penalty_event
                          FROM complete_timeline ct
                          LEFT JOIN event_timeline et ON (ct.game_id = et.game_id AND ct.event_datetime_minute = et.event_datetime_minute)
                        )
                        ORDER BY game_id, event_datetime_minute
                        """)

    
    # Summarizing only goals for our project visualization
    # condition_exp = functions.expr("""IF(is_scoring_event IS TRUE, 'Goal Scored!', IF(is_penalty_event IS TRUE, 'Penalty Recorded!', NULL))""")
    condition_exp = functions.expr(
      """IF(is_scoring_event IS TRUE, 'Goal Scored!', NULL)"""
    )

    final_output_df = final_output_df.withColumn('event', condition_exp)
    final_output_df = final_output_df.orderBy('game_id', 'event_datetime_minute')

    final_text = final_output_df

    # coalescing all the events for visualization on PowerBI
    final_text.coalesce(1).write.json(output)


    # spark-submit gamestats_converter.py game_stats gamestats_output_json
    # rm -r gamestats_output_json


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    spark = SparkSession.builder.appName('gamestats_converter').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(inputs, output)