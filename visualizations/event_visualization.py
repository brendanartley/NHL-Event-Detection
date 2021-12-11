from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import re
import sys
import json
import string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql.functions import col, max as max_, min as min_
#from pyspark.sql.types import StringType


def main(input_gamestats, input_reddit, input_twitter, input_classfication, output):

    # PART1. Construct a base timeline for each game

    # Define an explicit schema for the input file (gamestats)
    gamestats_schema = types.StructType([
    types.StructField('game_id', types.StringType()),
    types.StructField('event_datetime_minute', types.TimestampType()),
    types.StructField('count', types.IntegerType()),
    types.StructField('is_scoring_event', types.StringType()),
    types.StructField('is_penalty_event', types.StringType()),
    types.StructField('event', types.StringType())
     ])

    gamestates_df = spark.read.json(input_gamestats, schema=gamestats_schema)

    # Filtered by a specific game of interest
    gamestats_by_game = gamestates_df.where(gamestates_df['game_id'] == functions.lit('2021020001') )
    #gamestats_by_game = gamestates_df.where( (gamestates_df['game_id'] == functions.lit('2021020050')) | (gamestates_df['game_id'] == functions.lit('2020020013')) | (gamestates_df['game_id'] == functions.lit('2020020001')) )
    #gamestats_by_game = gamestates_df
    
    # Create a dataframe where each minute is present as a row from the game start time to the end
    complete_timeline_min_df = gamestats_by_game.groupBy('game_id').agg(min_("event_datetime_minute").alias('starttime'))
    complete_timeline_max_df = gamestats_by_game.groupBy('game_id').agg(max_("event_datetime_minute").alias('endtime'))
    complete_timeline_max_df = complete_timeline_max_df.withColumnRenamed('game_id','game_id_max')

    complete_timeline_df = complete_timeline_min_df.join(complete_timeline_max_df, complete_timeline_min_df['game_id'] == complete_timeline_max_df['game_id_max'])
    complete_timeline_df = complete_timeline_df.drop(complete_timeline_df['game_id_max']).cache()

    complete_timeline_df.show()
    
    # The gameid_list is cached for  to filter by game
    gameid_list_df = complete_timeline_df.select(complete_timeline_df['game_id'].alias('gameid_list')).cache()

    # Generate a complete timeline devided by minute
    complete_timeline_df = complete_timeline_df.select(complete_timeline_df['game_id'], functions.explode(functions.expr('sequence(starttime, endtime, interval 1 minute)')).alias('event_datetime_minute'))
    complete_timeline_df.createOrReplaceTempView('complete_timeline')

    

    # PART2. Reddit comment Consolidation
    print("Comments consolidation for Reddit Starts!!!")

    # Reddit data comments cosolidation
    reddit_comments_df = spark.read.parquet(input_reddit)

    #reddit_comments_filtered_df = reddit_comments_df.groupby('game_id').count()

    reddit_comments_filtered_df = reddit_comments_df.join(functions.broadcast(gameid_list_df), reddit_comments_df['game_id'] == gameid_list_df['gameid_list'], 'inner')
    reddit_comments_filtered_df = reddit_comments_filtered_df.select(reddit_comments_filtered_df['game_id'], reddit_comments_filtered_df['created_utc'])
    reddit_comments_filtered_df = reddit_comments_filtered_df.withColumn('created_utc_str', functions.date_format('created_utc',"MM/dd/yyyy HH:mm:ss"))

    reddit_comments_filtered_df = reddit_comments_filtered_df.withColumn('event_datetime_altered', functions.concat(functions.substring(reddit_comments_filtered_df['created_utc_str'],1,16), functions.lit(':00') ) )

    reddit_comments_filtered_df = reddit_comments_filtered_df.withColumn('event_datetime_minute', functions.to_timestamp(reddit_comments_filtered_df['event_datetime_altered'],'MM/dd/yyyy HH:mm:ss'))
    reddit_comments_filtered_df = reddit_comments_filtered_df.drop(reddit_comments_filtered_df['event_datetime_altered']).drop(reddit_comments_filtered_df['created_utc_str']).cache()
    



    # PART3. Tweets Consolidation
    print("Comments consolidation for Twitter Starts!!!")

    # Reddit data comments cosolidation
    twitter_comments_df = spark.read.parquet(input_twitter)
    #twitter_comments_df.printSchema()
    
    twiiter_comments_filtered_df = twitter_comments_df.join(functions.broadcast(gameid_list_df), twitter_comments_df['game_id'] == gameid_list_df['gameid_list'], 'inner')
    
    twiiter_comments_filtered_df = twiiter_comments_filtered_df.withColumn("created_utc_adjusted", twiiter_comments_filtered_df['created_utc'] - functions.expr("INTERVAL 7 HOURS"))
    twiiter_comments_filtered_df = twiiter_comments_filtered_df.select(twiiter_comments_filtered_df['game_id'], twiiter_comments_filtered_df['created_utc_adjusted'].alias('created_utc'))
    

    twiiter_comments_filtered_df = twiiter_comments_filtered_df.withColumn('created_utc_str', functions.date_format('created_utc',"MM/dd/yyyy HH:mm:ss"))

    twiiter_comments_filtered_df = twiiter_comments_filtered_df.withColumn('event_datetime_altered', functions.concat(functions.substring(twiiter_comments_filtered_df['created_utc_str'],1,16), functions.lit(':00') ) )

    twiiter_comments_filtered_df = twiiter_comments_filtered_df.withColumn('event_datetime_minute', functions.to_timestamp(twiiter_comments_filtered_df['event_datetime_altered'],'MM/dd/yyyy HH:mm:ss'))
    twiiter_comments_filtered_df = twiiter_comments_filtered_df.drop(twiiter_comments_filtered_df['event_datetime_altered']).drop(twiiter_comments_filtered_df['created_utc_str']).cache()



    
    # PART4. Union Reddit/Twitter Counts
    print("Group-by Count start!!!!!")

    combined_comments_df = reddit_comments_filtered_df.union(twiiter_comments_filtered_df)
    combined_comments_df = combined_comments_df.groupBy('game_id', 'event_datetime_minute').count()
    combined_comments_df.show()

    combined_comments_df.createOrReplaceTempView('event_timeline')

    final_count_df  = spark.sql("""
                        SELECT game_id, event_datetime_minute,
                               CASE WHEN count IS NULL THEN 0 ELSE count END AS count
                        FROM (
                          SELECT ct.game_id, ct.event_datetime_minute,
                               et.count
                          FROM complete_timeline ct
                          LEFT JOIN event_timeline et ON (ct.game_id = et.game_id AND ct.event_datetime_minute = et.event_datetime_minute)
                        )
                        ORDER BY game_id, event_datetime_minute
                        """)


    
    # PART5. Event Detection Merge into the base timeline
    print("Event Classification start!!!!!")

    # Event classification summarization part
    event_classificaiton_df = spark.read.parquet(input_classfication)
    event_classificaiton_df.printSchema()
    
    # Filter by sum_lex_count greater or equal to 3
    #event_classificaiton_df = event_classificaiton_df.where(event_classificaiton_df['sum_lex_count'] >= 3)

    event_classificaiton_df = event_classificaiton_df.withColumn('event_time', functions.from_unixtime(event_classificaiton_df['goal_time'])) 
    #event_classificaiton_df = event_classificaiton_df.withColumn('event_time', functions.from_unixtime(event_classificaiton_df['mid_window'])) 
    event_classificaiton_by_game = event_classificaiton_df.join(functions.broadcast(gameid_list_df), event_classificaiton_df['game_id'] == gameid_list_df['gameid_list'], 'inner')
    
    #event_classificaiton_by_game = event_classificaiton_by_game.withColumn('event_time_adjusted', event_classificaiton_by_game['event_time'] - functions.expr("INTERVAL 8 HOURS"))
    #event_classificaiton_by_game = event_classificaiton_by_game.select(event_classificaiton_by_game['game_id'],event_classificaiton_by_game['event_time_adjusted'].alias('event_time'))
    
    event_classificaiton_by_game = event_classificaiton_by_game.select(event_classificaiton_by_game['game_id'],event_classificaiton_by_game['event_time'])

    event_classificaiton_by_game = event_classificaiton_by_game.withColumn('event_time_str', functions.date_format('event_time',"MM/dd/yyyy HH:mm:ss"))
    event_classificaiton_by_game = event_classificaiton_by_game.withColumn('event_time', functions.concat(functions.substring(event_classificaiton_by_game['event_time_str'],1,16), functions.lit(':00') ) )
    event_classificaiton_by_game = event_classificaiton_by_game.withColumn('event_datetime_minute', functions.to_timestamp(event_classificaiton_by_game['event_time'],'MM/dd/yyyy HH:mm:ss'))

    event_classificaiton_by_game = event_classificaiton_by_game.select(event_classificaiton_by_game['game_id'].alias('game_id_ec'), event_classificaiton_by_game['event_datetime_minute'].alias('event_datetime_minute_ec'))
    event_classificaiton_final = event_classificaiton_by_game.orderBy('game_id_ec', 'event_datetime_minute_ec')


    event_count_df = event_classificaiton_final.groupBy('game_id_ec', 'event_datetime_minute_ec').count()
    event_count_df = event_count_df.withColumn('event', functions.lit('Goal Detected!!') )
    event_count_df = event_count_df.select(event_count_df['game_id_ec'],event_count_df['event_datetime_minute_ec'],event_count_df['event'])

    final_output_df = final_count_df.join(functions.broadcast(event_count_df), (final_count_df['game_id'] == event_count_df['game_id_ec']) & (final_count_df['event_datetime_minute'] == event_count_df['event_datetime_minute_ec']), 'leftouter')

    final_output_df = final_output_df.drop(final_output_df['game_id_ec']).drop(final_output_df['event_datetime_minute_ec'])
    #final_output_df.show()

    final_text = final_output_df
    final_text.coalesce(1).write.json(output)


    # spark-submit event_visualization.py gamestats_output_json nhl_comments_final_v2 unified_tweets new_tweet_with_prediction_v2 visualizaiton_output
    # rm -r visualizaiton_output


if __name__ == '__main__':
    input_gamestats = sys.argv[1]
    input_reddit = sys.argv[2]
    input_twitter = sys.argv[3]
    input_classfication = sys.argv[4]
    output = sys.argv[5]

    spark = SparkSession.builder.appName('event_visualization').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input_gamestats, input_reddit, input_twitter, input_classfication, output)