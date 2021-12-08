import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col

def main(inputs, output):
    tweet_path, event_path = inputs
    tweets = spark.read.parquet(tweet_path).where(col("language") == "en")
    unified = tweets.select(col("game_id"),
                            col("tweet").alias("body"),
                            col("created_at_utc_timestamp").alias("created_utc"),
                            col("home_team_name"),
                            col("away_team_name"),
                            functions.lit("twitter").alias("subreddit"),
                            col("id").alias("comment_id"), 
                            functions.concat(functions.regexp_replace(col("away_team_name"), " ", "_"),
                                            functions.lit("_at_"),
                                            functions.regexp_replace(col("away_team_name"), " ", "_")).alias("gname"),
                            functions.date_format(col("created_at_utc_timestamp"), "MM-dd-yy").alias("date"),
                            (functions.unix_timestamp(col("created_at_utc_timestamp"))).alias("created_unix")
                        )
    
    events = spark.read.parquet(event_path)
    events = events.where((col("is_scoring_event") == True) | (col("is_penalty_event") == True)).\
                withColumn("event_unix", functions.unix_timestamp(col("event_datetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    join_cond = [unified["game_id"] == events["game_id"],
                unified["created_unix"] - events["event_unix"] < 30,
                unified["created_unix"] - events["event_unix"] > 0]
    enhanced = unified.join(events, on=join_cond, how="left_outer").select(unified["*"], events["is_scoring_event"], events["is_penalty_event"], events["event_unix"], events["event_datetime"])
    enhanced = enhanced.groupBy(col("comment_id")).\
                        agg(functions.first("body").alias("body"),
                            functions.first("game_id").alias("game_id"),
                            functions.first("created_utc").alias("created_utc"),
                            functions.first("home_team_name").alias("home_team_name"),
                            functions.first("away_team_name").alias("away_team_name"),
                            functions.first("gname").alias("gname"),
                            functions.first("date").alias("date"),
                            functions.when(functions.max("is_scoring_event") == True, 1).otherwise(0).alias("recent_goal"),
                            functions.when(functions.max("is_penalty_event") == True, 1).otherwise(0).alias("recent_penalty"),
                        ).\
                        withColumn("subreddit", functions.lit("twitter"))
    enhanced.write.parquet(output, mode="overwrite")

if __name__ == '__main__':
    inputs = [sys.argv[1], sys.argv[2]]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
