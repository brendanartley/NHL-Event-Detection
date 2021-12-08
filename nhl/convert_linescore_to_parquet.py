import os
import sys
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions, types

import constants as c

def extract_match_info(linescore):
    try:
        match_digest = (linescore["gameID"],
                        linescore["teams"]["home"]["team"]["id"],
                        linescore["teams"]["home"]["team"]["name"],
                        linescore["teams"]["away"]["team"]["id"],
                        linescore["teams"]["away"]["team"]["name"],
                        linescore["periods"][0]["startTime"].replace("Z", "+00:00"),
                        linescore["periods"][-1]["endTime"].replace("Z", "+00:00"))
    except (KeyError, IndexError) as e:
        print(f"{linescore} lacks some attributes")
    else:
        return match_digest

def convert_linescore(spark, inputs, output):
    match_stats = []
    for dirpath, dirnames, filenames in os.walk(inputs):
        for filename in filenames:
            with open(os.path.join(dirpath, filename), "r") as f:
                for line in f:
                    digest = extract_match_info(json.loads(line))
                    if digest:
                        match_stats.append(digest)
    match_stats_df = spark.createDataFrame(match_stats, schema=c.STATS_LINESCORE_SCHEMA)
    match_stats_df.write.parquet(output, mode='overwrite')

if __name__ == "__main__":
    inputs = sys.argv[1]
    output = c.STATS_PARQUET_DIR
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    convert_linescore(spark, inputs, output)
