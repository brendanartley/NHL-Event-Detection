import sys, os, json
assert sys.version_info >= (3, 5)
import constants as c

from pyspark.sql import SparkSession, functions, types

with open(os.path.join(c.TWITTER_DIR, c.TWITTER_RAW_SCHEMA), "r") as schema_file:
    raw_schema = types.StructType.fromJson(json.load(schema_file))

def etl(tweets_path, gamestats_path, output):
    raw_tweets = spark.read.json(tweets_path, schema=raw_schema).withColumn('twitter_game_id', functions.split(functions.element_at(functions.split(functions.input_file_name(), "_"), -1), "\.")[0])

    game_stats = spark.read.parquet(gamestats_path)
    game_stats = game_stats.select(game_stats["*"],
                                functions.from_utc_timestamp(functions.substring(game_stats["start_time"], 1, 19), "UTC").alias("start_time_utc_timestamp"),
                                functions.from_utc_timestamp(functions.substring(game_stats["end_time"], 1, 19), "UTC").alias("end_time_utc_timestamp"))

    adjustment_schema = types.StructType([
        types.StructField('timezone_repr', types.StringType()),
        types.StructField('adjustment', types.StringType()),
    ])
    time_zone_adjustment = spark.createDataFrame([("PDT", "+0700"), ("PST", "+0800")], schema=adjustment_schema)
    time_zone_letter_repr = functions.substring(raw_tweets["created_at"], 21, 23)
    raw_tweets = raw_tweets.join(time_zone_adjustment, on=[time_zone_letter_repr == time_zone_adjustment["timezone_repr"]])

    utc_timestamp_no_timezone = functions.concat(raw_tweets["date"], functions.lit("T"), raw_tweets["time"])
    raw_tweets = raw_tweets.select(raw_tweets["*"], functions.from_utc_timestamp(utc_timestamp_no_timezone, raw_tweets["adjustment"]).alias("created_at_utc_timestamp"))

    join_cond = [raw_tweets["twitter_game_id"] == game_stats["game_id"], 
                raw_tweets["created_at_utc_timestamp"] >= game_stats["start_time_utc_timestamp"],
                raw_tweets["created_at_utc_timestamp"] <= game_stats["end_time_utc_timestamp"]]
    tweets_during_match = raw_tweets.join(game_stats, on=join_cond).drop(raw_tweets["twitter_game_id"])

    tweets_during_match.write.parquet(output, mode="overwrite")

if __name__ == '__main__':
    tweets_path = sys.argv[1]
    stats_path = sys.argv[2]
    if len(sys.argv) > 3:
        output = sys.argv[3]
    else:
        output = os.path.join(c.TWITTER_DIR, c.TWITTER_PARQUET_DIR)
    spark = SparkSession.builder.appName('twitter cleaning').getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(tweets_path, stats_path, output)
