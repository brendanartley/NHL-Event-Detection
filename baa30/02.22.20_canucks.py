import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):

    #comments data
    df = spark.read.parquet("./nhl_comments_feb_2020")
    canucks_comments_df = df.where(df['subreddit'] == "canucks")

    #submissions data
    can_sdf = spark.read.parquet("./nhl_2020_subs")

    #getting only canucks in feb_2020
    canucks_feb_game_threads = (can_sdf.where((can_sdf['link_flair_text'] == "GAME THREAD") 
                                & (can_sdf["month"]==2) 
                                & (can_sdf["title"].startswith("GT:"))))

    #list of gamethread ids (small list <20 here so we can use .collect())
    #id's that start with t3_ are "first tier" comments (not a nested comment)
    feb_gt_ids = canucks_feb_game_threads.select(canucks_feb_game_threads["id"]).rdd.map(lambda x: "t3_" + x[0]).collect()

    #if we want to include nested comments, change "parent_id" to "link_id"
    canucks_gt_comments = canucks_comments_df.where(canucks_comments_df["parent_id"].isin(feb_gt_ids))

    #getting a sample of comments from one game thread
    # GT: BOSTON BRUINS @ VANCOUVER CANUCKS - 02/22/2020 - 07:00 PM 
    sample_df = canucks_gt_comments.where(canucks_gt_comments["link_id"] == "t3_f83dnv").select(canucks_gt_comments["created_utc"]).coalesce(1)
    sample_df.write.parquet("./sample_gt_comments")
    


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)