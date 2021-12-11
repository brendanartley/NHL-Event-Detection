import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs1, inputs2, output):

    #read parquet files
    root_df = spark.read.parquet(inputs1)
    nhl_df = spark.read.parquet(inputs2)

    #day of game for nhl_games + removing unregular NHL teams/games + converting timestamp format
    nhl_df = (nhl_df.withColumn("simple_date_api", functions.to_timestamp("start_time").cast(types.DateType()))
              .where((nhl_df["home_team_name"] != "Team Atlantic") & 
                     (nhl_df["home_team_name"] != "Team Central") & 
                     (nhl_df["home_team_name"] != "Team Pacific") &
                     (nhl_df["home_team_name"] != nhl_df["away_team_name"]))
                    .withColumn("start_time", functions.unix_timestamp(functions.to_timestamp("start_time").cast(types.TimestampType())))
                    .withColumn("end_time", functions.unix_timestamp(functions.to_timestamp("end_time").cast(types.TimestampType()))))

    #day of game for reddit 
    reddit_df = (root_df.withColumn("scu_adjusted", 
        functions.when((functions.col("subreddit")  == "BlueJackets"), (root_df["sub_created_utc"].cast(types.IntegerType()) + 14400).cast(types.StringType()))
                 .when((functions.col("subreddit")  == "winnipegjets"), (root_df["sub_created_utc"].cast(types.IntegerType()) + 14400).cast(types.StringType()))
                 .when((functions.col("subreddit")  == "penguins"), (root_df["sub_created_utc"].cast(types.IntegerType()) + 14400).cast(types.StringType()))
                 .when((functions.col("subreddit")  == "DallasStars"), (root_df["sub_created_utc"].cast(types.IntegerType()) + 10600).cast(types.StringType()))
                 .otherwise(root_df["sub_created_utc"])))

    reddit_df = reddit_df.withColumn("simple_date", functions.from_unixtime("scu_adjusted").cast(types.DateType())).cache()


    #getting team names from title for hockey_subreddits
    hockey_sub_df = reddit_df.where(reddit_df["subreddit"] == "hockey").distinct()
    hockey_sub_df = hockey_sub_df.withColumn("part", functions.substring("sub_title", 14, 100))
    hockey_sub_df = hockey_sub_df.withColumn("part", functions.split(hockey_sub_df["part"], " - ")[0])
    hockey_sub_df = hockey_sub_df.withColumn("part", functions.regexp_replace("part", "[^a-zA-Z ]", ""))
    hockey_sub_df = (hockey_sub_df.select("subreddit", "simple_date", "body", "created_utc", "id",
                  functions.trim(functions.split("part", " at ")[0]).alias("team_1"), 
                  functions.trim(functions.split("part", " at ")[1]).alias("team_2")))

    #converting reddit_data to NHL_API team name scheme
    hockey_sub_df = (hockey_sub_df.withColumn("team_1", functions.when(hockey_sub_df["team_1"] == "Montral Canadiens", "Montréal Canadiens")
                                            .when(hockey_sub_df["team_1"] == "Montreal Canadiens", "Montréal Canadiens")
                                            .when(hockey_sub_df["team_1"] == "St Louis Blues", "St. Louis Blues")
                                            .otherwise(hockey_sub_df["team_1"])))

    hockey_sub_df = (hockey_sub_df.withColumn("team_2", functions.when(hockey_sub_df["team_2"] == "Montral Canadiens", "Montréal Canadiens")
                                            .when(hockey_sub_df["team_2"] == "Montreal Canadiens", "Montréal Canadiens")
                                            .when(hockey_sub_df["team_2"] == "St Louis Blues", "St. Louis Blues")
                                            .otherwise(hockey_sub_df["team_2"])))


    team_subs_df = reddit_df.where(reddit_df["subreddit"] != "hockey")

    team_subs_df = (team_subs_df.withColumn("team_name", 
                functions.when(team_subs_df["subreddit"] == "SanJoseSharks", "San Jose Sharks")
                .when(team_subs_df["subreddit"] == "CalgaryFlames", "Calgary Flames")
                .when(team_subs_df["subreddit"] == "sabres", "Buffalo Sabres")
                .when(team_subs_df["subreddit"] == "winnipegjets", "Winnipeg Jets")
                .when(team_subs_df["subreddit"] == "TampaBayLightning", "Tampa Bay Lightning")
                .when(team_subs_df["subreddit"] == "losangeleskings", "Los Angeles Kings")
                .when(team_subs_df["subreddit"] == "ColoradoAvalanche", "Colorado Avalanche")
                .when(team_subs_df["subreddit"] == "canes", "Carolina Hurricanes")
                .when(team_subs_df["subreddit"] == "rangers", "New York Rangers")
                .when(team_subs_df["subreddit"] == "goldenknights", "Vegas Golden Knights")
                .when(team_subs_df["subreddit"] == "Predators", "Nashville Predators")
                .when(team_subs_df["subreddit"] == "caps", "Washington Capitals")
                .when(team_subs_df["subreddit"] == "FloridaPanthers", "Florida Panthers")
                .when(team_subs_df["subreddit"] == "leafs", "Toronto Maple Leafs")
                .when(team_subs_df["subreddit"] == "canucks", "Vancouver Canucks")
                .when(team_subs_df["subreddit"] == "stlouisblues", "St. Louis Blues")
                .when(team_subs_df["subreddit"] == "BlueJackets", "Columbus Blue Jackets")
                .when(team_subs_df["subreddit"] == "BostonBruins", "Boston Bruins")
                .when(team_subs_df["subreddit"] == "AnaheimDucks", "Anaheim Ducks")
                .when(team_subs_df["subreddit"] == "wildhockey", "Minnesota Wild")
                .when(team_subs_df["subreddit"] == "OttawaSenators", "Ottawa Senators")
                .when(team_subs_df["subreddit"] == "penguins", "Pittsburgh Penguins")
                .when(team_subs_df["subreddit"] == "devils", "New Jersey Devils")
                .when(team_subs_df["subreddit"] == "EdmontonOilers", "Edmonton Oilers")
                .when(team_subs_df["subreddit"] == "hawks", "Chicago Blackhawks")
                .when(team_subs_df["subreddit"] == "Habs", "Montréal Canadiens")
                .when(team_subs_df["subreddit"] == "DallasStars", "Dallas Stars")
                .when(team_subs_df["subreddit"] == "NewYorkIslanders", "New York Islanders")
                .when(team_subs_df["subreddit"] == "DetroitRedWings", "Detroit Red Wings")
                .when(team_subs_df["subreddit"] == "Flyers", "Philadelphia Flyers")
                .when(team_subs_df["subreddit"] == "Coyotes", "Arizona Coyotes")
                .when(team_subs_df["subreddit"] == "SeattleKraken", "Seattle Kraken")))


    #Joining NHL_API with the reddit data - only keeping comments during game
    hs_df = hockey_sub_df.join(nhl_df, how='inner', on=(((hockey_sub_df["simple_date"]==nhl_df["simple_date_api"]) & (hockey_sub_df["team_1"]==nhl_df["home_team_name"])) | ((hockey_sub_df["simple_date"]==nhl_df["simple_date_api"]) & (hockey_sub_df["team_1"]==nhl_df["away_team_name"]))))
    hs_df = hs_df.where((hs_df["created_utc"] > hs_df["start_time"]) & (hs_df["created_utc"] < hs_df["end_time"]))
    hs_df = hs_df.select(hs_df["game_id"], hs_df["body"], functions.from_unixtime(hs_df["created_utc"]).cast(types.TimestampType()).alias("created_utc"), hs_df["simple_date"].alias("date"), hs_df["home_team_name"], hs_df["away_team_name"], hs_df["subreddit"], hs_df["id"].alias("comment_id"))

    ts_df = team_subs_df.join(nhl_df, how='inner', on=(((team_subs_df["simple_date"]==nhl_df["simple_date_api"]) & (team_subs_df["team_name"]==nhl_df["home_team_name"])) | ((team_subs_df["simple_date"]==nhl_df["simple_date_api"]) & (team_subs_df["team_name"]==nhl_df["away_team_name"]))))
    ts_df = ts_df.where((ts_df["created_utc"] > ts_df["start_time"]) & (ts_df["created_utc"] < ts_df["end_time"]))
    ts_df = ts_df.select(ts_df["game_id"], ts_df["body"], functions.from_unixtime(ts_df["created_utc"]).cast(types.TimestampType()).alias("created_utc"), ts_df["simple_date"].alias("date"), ts_df["home_team_name"], ts_df["away_team_name"], ts_df["subreddit"], ts_df["id"].alias("comment_id"))

    #repartition to 1 speed up the program by 10x (105mins -> 8mins)
    final = hs_df.union(ts_df).cache()
    final = final.withColumn("gname", functions.concat_ws("_at_", functions.regexp_replace(final["away_team_name"], " ", "_"), functions.regexp_replace(final["home_team_name"], " ", "_"))).repartition(1)

    #writing files to output
    final.write.partitionBy("date", "gname").parquet(output, mode="append")

if __name__ == '__main__':
    inputs1 = "./nhl_gamethreads_comments_joined"
    inputs2 = "./nhlapi_2019_to_present"
    output = "nhl_comments_final"
    spark = SparkSession.builder.appName('API + Reddit Join').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs1, inputs2, output)
