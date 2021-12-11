import os
import sys
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions, types

import constants as c

event_schema = types.StructType([
    types.StructField("event_index", types.IntegerType()),
    types.StructField("event_name", types.StringType()),
    types.StructField("event_datetime", types.StringType()),
    types.StructField("game_id", types.StringType()),
    types.StructField("game_date", types.StringType()),
    types.StructField("home_team_name", types.StringType()),
    types.StructField("away_team_name", types.StringType()),
    types.StructField("team_summary", types.StringType()),
    types.StructField("is_scoring_event", types.StringType()),
    types.StructField("is_penalty_event", types.StringType()),
])

def extract_events(pair):
    livefeed_obj = json.loads(pair[1])
    event_list = []
    scoring_event_index = set(livefeed_obj["liveData"]["plays"]["scoringPlays"])
    penalty_event_index = set(livefeed_obj["liveData"]["plays"]["penaltyPlays"])
    for i, event in enumerate(livefeed_obj["liveData"]["plays"]["allPlays"]):
        event_list.append((i, event["result"]["event"], event["about"]["dateTime"],
                        livefeed_obj["gamePk"], livefeed_obj["gameData"]["datetime"]["dateTime"][:10], livefeed_obj["gameData"]["teams"]["home"]["name"],
                        livefeed_obj["gameData"]["teams"]["away"]["name"],
                        (livefeed_obj["gameData"]["teams"]["home"]["name"] + "_v_" + livefeed_obj["gameData"]["teams"]["away"]["name"]).replace(" ", "-"),
                        i in scoring_event_index,
                        i in penalty_event_index))
    return event_list

def convert_linescore(inputs, output):
    raw_text_rdd = sc.wholeTextFiles(inputs)
    events_df = spark.createDataFrame(raw_text_rdd.flatMap(extract_events), schema=event_schema)
    events_df.write.partitionBy("game_date", "game_id", "team_summary").mode("overwrite").parquet(output)

if __name__ == "__main__":
    inputs = sys.argv[1]
    output = c.STATS_DEFAULT_PARQUET_DIR[c.DetailLevel.LIVE_FEED]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    convert_linescore(inputs, output)
