import sys
from datetime import datetime, timedelta
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs1, inputs2, output):

    #read files
    df = spark.read.parquet(inputs1)
    api_df = spark.read.option("basePath", inputs2).parquet(inputs2)

    #get timestamp in PST and goals + penalties
    events = api_df.where((api_df["event_name"] == "Goal") | (api_df["event_name"] == "Penalty")).\
                withColumn("event_datetime", functions.to_timestamp("event_datetime"))
    events = events.groupBy("game_id").\
                agg(functions.collect_list("event_datetime").alias("collect_list_event_datetime"),
                    functions.collect_list("event_name").alias("collect_list_event_name")
                )

    df_joined = df.join(events, on="game_id")

    #defining UDF
    def recent_event(time_arr, event_arr, created_time):
        """
        Return tuple of two elements to indicate if there was a goal and/or penalty
        in the last 30 seconds. 

        ex. (was goal, was not penalty) --> (1, 0)
        ex. (no goal, no penalty) --> (0, 0)
        """
        pair = [0, 0]
        for time, event in zip(time_arr, event_arr):
            if created_time > time and created_time < time + timedelta(seconds=60):
                if event == "Penalty":
                    pair[1] = 1
                else:
                    pair[0] = 1
        return pair
    recent_event = functions.udf(recent_event, types.ArrayType(types.IntegerType()))

    #Selecting relevant columns
    final = df_joined.withColumn("recent_event_arr", recent_event(df_joined["collect_list_event_datetime"], df_joined['collect_list_event_name'], df_joined["created_utc"]))
    final = final.select(final["game_id"], 
                         final["body"], 
                         final["created_utc"], 
                         final["home_team_name"], 
                         final["away_team_name"], 
                         final["subreddit"],
                         final["comment_id"],
                         final["recent_event_arr"][0].alias("recent_goal"),
                         final["recent_event_arr"][1].alias("recent_penalty"),
                         final["gname"], #for partition on output
                         final["date"])  #for partition on output

    final.write.partitionBy("date", "gname").parquet(output, mode="append")

if __name__ == '__main__':
    #argument aids
    if len(sys.argv) < 2:
        inputs1 = "./nhl_comments_final"
    else:
        inputs1 = sys.argv[1]

    if len(sys.argv) < 3:
        inputs2 = "./game_stats"
    else:
        inputs2 = sys.argv[2]

    if len(sys.argv) < 4:
        output = "./nhl_comments_final_v2"
    else:
        output = sys.argv[3]

    spark = SparkSession.builder.appName('NHL subs + comms join').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs1, inputs2, output)
