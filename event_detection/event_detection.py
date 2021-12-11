import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql import types

buffer = 1
sub_window_sizes = [6, 10, 20, 30, 60]

COMMENTS_PER_SECOND = 1
LEFT_RIGHT_RATIO = 1

def sub_windowing(p_df):
    p_df = p_df.sort_values(by=["t_plus"], ascending=True)
    for sub_window_size in sub_window_sizes:
        left_half_size = p_df["t_plus"].between(0, sub_window_size // 2, inclusive="left").sum()
        right_half_size = p_df["t_plus"].between(sub_window_size // 2, sub_window_size, inclusive="left").sum()
        if left_half_size + right_half_size >= sub_window_size * COMMENTS_PER_SECOND \
            and left_half_size < right_half_size * LEFT_RIGHT_RATIO:
            break
    else:
        p_df["valid_window_size"] = [None] * len(p_df.index)
        return p_df
    
    p_df["valid_window_size"] = [sub_window_size] * len(p_df.index)
    return p_df

def is_useful(df):
    condition = functions.when((functions.col('created_unix') > functions.col('mid_window') - buffer) & \
        (functions.col('created_unix') < functions.col('mid_window') + buffer),True).otherwise(False)
    df = df.withColumn('is_useful', condition)
    return df

def main(inputs,output):

    comments = spark.read.parquet(inputs).withColumn("t_plus", (functions.col("created_unix") - functions.col("in_window_starting_at")))
    validated_windowing_shcmea = types.StructType(comments.schema.fields + [types.StructField("valid_window_size", types.LongType())])
    
    comments = comments.groupBy("game_id", "in_window_starting_at").\
                    applyInPandas(sub_windowing, schema=validated_windowing_shcmea)

    
    comments_windows = comments.select('game_id', 'body','created_unix','in_window_starting_at','valid_window_size')

    comments_windows = comments_windows.filter(comments_windows['valid_window_size'].isNotNull()).\
                                    withColumn("mid_window", functions.col("in_window_starting_at") + functions.floor(functions.col("valid_window_size") / 2))

    comments_windows_useful = is_useful(comments_windows)
    comments_windows_useful.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
