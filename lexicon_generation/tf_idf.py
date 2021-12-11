import sys
assert sys.version_info >= (3, 5)

import pandas as pd

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col

def main(inputs, output):
    processed_path, game_stats_path = inputs
    processed = spark.read.parquet(processed_path)
                    
    game_stats = spark.read.parquet(game_stats_path)
    goal_stats = game_stats.where(game_stats["event_name"] == "Goal").\
                        withColumn("event_unix", 
                                    functions.unix_timestamp(game_stats["event_datetime"], 
                                                            "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    join_cond = [processed["game_id"] == goal_stats["game_id"],
                processed["mid_window"] - goal_stats["event_unix"] > 0,
                processed["mid_window"] - goal_stats["event_unix"] < 60]
    windows_with_goal = processed.join(goal_stats, on=join_cond, how="left_outer").\
                                select(processed["*"], goal_stats["event_unix"], (processed["mid_window"] - goal_stats["event_unix"])).\
                                groupBy(col("game_id"), col("mid_window")).\
                                agg(functions.max(col("event_unix")).alias("event_unix"),
                                    functions.first(col("comment_summary")).alias("comment_summary"))
    
    all_windows = windows_with_goal.select(col("*"), col("comment_summary").alias("document"), functions.flatten(col("comment_summary")).alias("words")).\
                                    drop(col("comment_summary")).\
                                    withColumn("row_id", functions.monotonically_increasing_id())
    document_count = all_windows.count()

    unfolded = all_windows.select(col("row_id"), 
                                col("document"),
                                col("event_unix").alias("near_goal"),
                                functions.explode(col("words")).alias("token"))

    term_freq = unfolded.groupBy(col("row_id"), col("token")).\
                        agg(functions.count(col("document")).alias("tf"), 
                            functions.first(col("near_goal")).alias("near_goal"))
    doc_freq = unfolded.groupBy(col("token")).\
                        agg(functions.countDistinct(col("row_id")).alias("df"))

    inverse_doc_freq = doc_freq.select(col("*"), (functions.log((document_count + 1) / (col("df") + 1))).alias("idf"))

    tf_idf = term_freq.join(inverse_doc_freq, on="token").\
                    withColumn("tf_idf", col("tf") * col("idf")).orderBy(col("tf_idf").desc())
    tf_idf.write.parquet(output, mode="overwrite")

if __name__ == '__main__':
    inputs = [sys.argv[1], sys.argv[2]]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('lexicon generation').getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
