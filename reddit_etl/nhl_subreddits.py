import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs):

    #define schema
    submissions_schema = types.StructType([
        types.StructField('archived', types.BooleanType(), True),
        types.StructField('author', types.StringType(), True),
        types.StructField('author_flair_css_class', types.StringType(), True),
        types.StructField('author_flair_text', types.StringType(), True),
        types.StructField('created', types.LongType(), True),
        types.StructField('created_utc', types.StringType(), True),
        types.StructField('distinguished', types.StringType(), True),
        types.StructField('domain', types.StringType(), True),
        types.StructField('downs', types.LongType(), True),
        types.StructField('edited', types.BooleanType(), True),
        types.StructField('from', types.StringType(), True),
        types.StructField('from_id', types.StringType(), True),
        types.StructField('from_kind', types.StringType(), True),
        types.StructField('gilded', types.LongType(), True),
        types.StructField('hide_score', types.BooleanType(), True),
        types.StructField('id', types.StringType(), True),
        types.StructField('is_self', types.BooleanType(), True),
        types.StructField('link_flair_css_class', types.StringType(), True),
        types.StructField('link_flair_text', types.StringType(), True),
        types.StructField('media', types.StringType(), True),
        types.StructField('name', types.StringType(), True),
        types.StructField('num_comments', types.LongType(), True),
        types.StructField('over_18', types.BooleanType(), True),
        types.StructField('permalink', types.StringType(), True),
        types.StructField('quarantine', types.BooleanType(), True),
        types.StructField('retrieved_on', types.LongType(), True),
        types.StructField('saved', types.BooleanType(), True),
        types.StructField('score', types.LongType(), True),
        types.StructField('secure_media', types.StringType(), True),
        types.StructField('selftext', types.StringType(), True),
        types.StructField('stickied', types.BooleanType(), True),
        types.StructField('subreddit', types.StringType(), True),
        types.StructField('subreddit_id', types.StringType(), True),
        types.StructField('thumbnail', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('ups', types.LongType(), True),
        types.StructField('url', types.StringType(), True),
        # types.StructField('year', types.IntegerType(), False),
        # types.StructField('month', types.IntegerType(), False),
        ])

    #relevant subreddits
    nhl_subs = [
            "hockey",
            "leafs",
            "hawks",
            "DetroitRedWings",
            "penguins",
            "canucks",
            "BostonBruins",
            "Flyers",
            "Habs",
            "SanJoseSharks",
            "caps",
            "NewYorkIslanders",
            "wildhockey",
            "rangers",
            "devils",
            "BlueJackets",
            "TampaBayLightning",
            "EdmontonOilers",
            "ColoradoAvalanche",
            "stlouisblues",
            "goldenknights",
            "AnaheimDucks",
            "winnipegjets",
            "Predators",
            "canes",
            "sabres",
            "CalgaryFlames",
            "losangeleskings",
            "OttawaSenators",
            "DallasStars",
            "Coyotes",
            "FloridaPanthers",
            "SeattleKraken"]

    ##reading both 2020 + 2021 comments w/ union method as per docs
    df_2020 = spark.read.json(inputs[0], schema=submissions_schema)
    df_2021 = spark.read.json(inputs[1], schema=submissions_schema)
    df = df_2020.union(df_2021)

    #selecting only those comments in NHL subreddits
    nhl_game_subs = df.filter(df['subreddit'].isin(nhl_subs))

    #output to parquet
    nhl_game_subs.write.parquet("nhl_subreddit_submissions")

if __name__ == '__main__':
    #getting 2020 + 2021 data from HDFS
    inputs = ["/courses/datasets/reddit_submissions_repartitioned/year=2020", "/courses/datasets/reddit_submissions_repartitioned/year=2021"]
    spark = SparkSession.builder.appName('filter NHL submissions').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)