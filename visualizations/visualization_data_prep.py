import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs):

    #reading inputs + adding date(day)
    df = spark.read.parquet(inputs)
    df = df.withColumn("date", functions.to_timestamp("created_utc").cast(types.DateType()).alias("date"))

    #creating time bin intervals
    interval = 30 # in seconds
    test_df = (df.withColumn('time_interval',
        functions.from_unixtime(functions.floor(functions.unix_timestamp(df["created_utc"]) / interval) * interval))
        .groupBy('game_id', 'time_interval')
        .agg(functions.count("body").alias("bin_count")))

    # Needs to be one file for visualization in tableau..
    test_df.repartition(1).write.csv("visualization_csv_sample")

if __name__ == '__main__':
    inputs = "./nhl_comments_final"
    spark = SparkSession.builder.appName('filter NHL comments').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)