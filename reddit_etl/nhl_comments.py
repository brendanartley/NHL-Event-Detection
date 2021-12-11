import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs):

    #defining schema
    comments_schema = types.StructType([
        types.StructField('archived', types.BooleanType(), True),
        types.StructField('author', types.StringType(), True),
        types.StructField('author_flair_css_class', types.StringType(), True),
        types.StructField('author_flair_text', types.StringType(), True),
        types.StructField('body', types.StringType(), True),
        types.StructField('controversiality', types.LongType(), True),
        types.StructField('created_utc', types.StringType(), True),
        types.StructField('distinguished', types.StringType(), True),
        types.StructField('downs', types.LongType(), True),
        types.StructField('edited', types.StringType(), True),
        types.StructField('gilded', types.LongType(), True),
        types.StructField('id', types.StringType(), True),
        types.StructField('link_id', types.StringType(), True),
        types.StructField('name', types.StringType(), True),
        types.StructField('parent_id', types.StringType(), True),
        types.StructField('retrieved_on', types.LongType(), True),
        types.StructField('score', types.LongType(), True),
        types.StructField('score_hidden', types.BooleanType(), True),
        types.StructField('subreddit', types.StringType(), True),
        types.StructField('subreddit_id', types.StringType(), True),
        types.StructField('ups', types.LongType(), True),
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

    #reading both 2020 + 2021 comments w/ union method as per docs
    df_2020 = spark.read.json(inputs[0], schema=comments_schema)
    df_2021 = spark.read.json(inputs[1], schema=comments_schema)
    df = df_2020.union(df_2021)

    #selecting only those comments in NHL subredditss
    only_nhl_coms = df.filter(df['subreddit'].isin(nhl_subs))

    #output to parquet
    only_nhl_coms.write.parquet("nhl_subreddit_comments")

if __name__ == '__main__':
    inputs = ["/courses/datasets/reddit_comments_repartitioned/year=2020", "/courses/datasets/reddit_comments_repartitioned/year=2021"]
    spark = SparkSession.builder.appName('filter NHL comments').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)