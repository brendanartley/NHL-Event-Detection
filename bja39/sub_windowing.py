import sys
assert sys.version_info >= (3, 5)

import pandas

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *

def main(inputs, output):
    comments = spark.read.parquet(inputs).withColumn("t_plus", (col("created_unix") - col("in_window_starting_at")))
    sub_window_sizes = [6, 10, 20, 30, 60]
    validated_windowing_shcmea = types.StructType(comments.schema.fields + [types.StructField("valid_window_size", types.LongType())])
    
    def sub_windowing(p_df):
        p_df = p_df.sort_values(by=["t_plus"], ascending=True)
        # print(len(p_df.index))
        for sub_window_size in sub_window_sizes:
            left_half_size = p_df["t_plus"].between(0, sub_window_size // 2, inclusive="left").sum()
            right_half_size = p_df["t_plus"].between(sub_window_size // 2, sub_window_size, inclusive="left").sum()
            if left_half_size + right_half_size >= sub_window_size and left_half_size < right_half_size:
                break
        else:
            p_df["valid_window_size"] = [None] * len(p_df.index)
            return p_df
        
        p_df["valid_window_size"] = [sub_window_size] * len(p_df.index)
        return p_df

    comments = comments.groupBy("in_window_starting_at").\
                    applyInPandas(sub_windowing, schema=validated_windowing_shcmea)

    comments.write.parquet(output, mode="overwrite")

if __name__ == '__main__':
    # input path, window size, sliding step size
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
