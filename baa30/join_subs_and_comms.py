import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, inputs2, output):

    #read parquet files
    root_df = spark.read.parquet(inputs1)
    cdf = spark.read.parquet(inputs2)

    #adding prefix in order to join with comments
    root_df2 = root_df.withColumn("id_prefix", functions.lit("t3_"))
    root_df3 = root_df2.withColumn("l_id", functions.concat(root_df2["id_prefix"], root_df2["id"]))
    root_df4 = root_df3.select("title", "num_comments", "created_utc", "l_id", "id", "subreddit")

    #differentiating submissions columns from comments columns
    root_df5 = root_df4.select([functions.col(c).alias("sub_" + c) for c in root_df4.columns])

    #broadcast join submissions to the comments data
    relevant_comments = cdf.join(functions.broadcast(root_df5), cdf["link_id"] == root_df5["sub_l_id"])

    relevant_comments.write.parquet(output)

    #Is there a problem with having a ton of files when we write to parquet?
    #Need to repartition after joining the comments, some files are much larger than others. 

if __name__ == '__main__':
    inputs1 = "./nhl_gamethreads_submissions"
    inputs2 = "./nhl_subreddit_comments"
    output = "nhl_gamethreads_comments_joined"
    spark = SparkSession.builder.appName('NHL subs + comms join').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs1, inputs2,  output)
