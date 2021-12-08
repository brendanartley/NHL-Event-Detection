import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    comment_path, window_size, step_size = inputs
    comments = spark.read.parquet(comment_path)
    comments = comments.select(comments["*"],
                            functions.unix_timestamp("created_utc").alias("created_unix"),
                            functions.lit(window_size).alias("window_size"),
                            functions.lit(step_size).alias("step_size"))
    comments = comments.select(comments["*"], 
                            (comments["created_unix"] - window_size).alias("first_window"),
                            comments["created_unix"].alias("last_window")
                        )
    
    with_window_shema = types.StructType(comments.schema.fields + [types.StructField("window_indices", types.ArrayType(types.LongType()))])
    
    def produce_window_index_for_group(data):
        indices = []
        for start, end in zip(data["first_window"], data["last_window"]):
            indices.append([i for i in range(start, end + 1, step_size)])
        data["window_indices"] = indices
        return data

    comments = comments.groupBy("game_id").\
                    applyInPandas(produce_window_index_for_group, schema=with_window_shema)
    comments = comments.withColumn("in_window_starting_at", functions.explode_outer("window_indices")).\
                    drop("window_indices", "first_window", "last_window")
    comments.write.partitionBy("game_id").parquet(output, mode="overwrite")

if __name__ == '__main__':
    # input path, window size, sliding step size
    inputs = [sys.argv[1], int(sys.argv[2]), int(sys.argv[3])]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
