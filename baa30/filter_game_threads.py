import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):

    #need to overwrite then append

    #Loading Data
    root_df = spark.read.parquet(inputs)

    #"hockey"
    df = root_df.where(root_df["subreddit"] == "hockey")

    bad_ids = ["hgimtc", "etl047", "i7dkjf", 
               "mdu813", "ezlond", "m03lwe", 
               "fqv27p", "l3kugi", "etyba3",
               "nqxy43", "eu13bf", "etzmh3",
               "eu0c9d", "exw2s5"]

    final = (df.where(df["title"].startswith("Game Thread:") & 
                     (df["link_flair_text"].startswith("[GDT]")) & 
                     (~df["title"].contains("NWHL")) & 
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final = final.where(~final["id"].isin(bad_ids))
    final.write.parquet(output, mode='overwrite')

    #"leafs"
    bad_ids = ["etzx8d"]

    df = root_df.where(root_df["subreddit"] == "leafs")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]") & 
                     (~df["id"].isin(bad_ids))).select("title","num_comments", "created_utc", "id", "subreddit"))
    
    final.write.parquet(output, mode='append')

    #"hawks"
    df = root_df.where(root_df["subreddit"] == "hawks")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"DetroitRedWings"
    bad_ids = ["fhek5g"]

    df = root_df.where(root_df["subreddit"] == "DetroitRedWings")

    final = (df.where(df["title"].startswith("Game Thread:") & 
                     (~df["title"].contains("NHL Draft Lottery")) &
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]") &
                     (~df["id"].isin(bad_ids))).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"penguins"
    df = root_df.where(root_df["subreddit"] == "penguins")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"canucks"
    df = root_df.where(root_df["subreddit"] == "canucks")

    final = (df.where((df['link_flair_text'] == "GAME THREAD") & 
                      (df["title"].startswith("GT:")) & 
                      (df["selftext"]!="[removed]") & 
                      (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"BostonBruins"
    bad_ids = ["fi9b9h","finmwg","fj9xhf","fjsnjl","fkg5sr","fkzr3y","flix5s","fm480z","fmq10u","fofstl","mtjx25"]

    df = root_df.where(root_df["subreddit"] == "BostonBruins")

    final = (df.where((df["title"].startswith("GDT:")) &
                      ((df["link_flair_text"] == "GDT: Away") | 
                       (df["link_flair_text"] == "GDT: Home") | 
                       (df["link_flair_text"] == "Game Thread")) & 
                      (df["selftext"]!="[removed]") &
                      (df["selftext"]!="[deleted]") & 
                      (~df["id"].isin(bad_ids))).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"Flyers"
    bad_ids = ["jaq141","jao3o3","fow2rf"]

    df = root_df.where(root_df["subreddit"] == "Flyers")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]"))).select("title","num_comments", "created_utc", "id", "subreddit")
    
    final = final.where(~final["id"].isin(bad_ids))
    final.write.parquet(output, mode='append')

    #"Habs"
    bad_ids = ["fh68hd","f8pr0r","kuq2zr"]

    df = root_df.where(root_df["subreddit"] == "Habs")

    final = (df.where(((df["title"].startswith("Game Thread:")) | 
                        (df["title"].startswith("[GDT]"))) &  
                       (df["selftext"]!="[removed]") & 
                       (df["selftext"]!="[deleted]"))).select("title","num_comments", "created_utc", "id", "subreddit")
    
    final = final.where(~final["id"].isin(bad_ids))
    final.write.parquet(output, mode='append')

    #"SanJoseSharks"
    df = root_df.where(root_df["subreddit"] == "SanJoseSharks")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"caps"
    df = root_df.where(root_df["subreddit"] == "caps")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"NewYorkIslanders" - Really good data
    df = root_df.where(root_df["subreddit"] == "NewYorkIslanders")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"wildhockey"
    df = root_df.where(root_df["subreddit"] == "wildhockey")

    final = (df.where(((df["title"].startswith("Game Thread:")) | 
                        (df["title"].startswith("GDT:"))) &
                       (~df["title"].contains("(NHL20 Simulation)")) &
                       (df["selftext"]!="[removed]") &
                       (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"rangers"
    df = root_df.where(root_df["subreddit"] == "rangers")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"devils"
    df = root_df.where(root_df["subreddit"] == "devils")

    final = (df.where(df["title"].startswith("Game Thread ") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"BlueJackets"
    df = root_df.where(root_df["subreddit"] == "BlueJackets")

    final = (df.where(df["title"].startswith("Game Thread") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"TampaBayLightning" - Not great data
    df = root_df.where(root_df["subreddit"] == "TampaBayLightning")

    final = (df.where(df["title"].startswith("GDT: ") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')   

    #"EdmontonOilers"
    df = root_df.where(root_df["subreddit"] == "EdmontonOilers")

    final = (df.where((df["title"].startswith("Game Day Thread")) & 
                     (df["title"].contains("Oilers")) & 
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]"))
                     .union(df.where((df["title"].startswith("Game Day Talk")) & 
                                     (df["selftext"]!="[removed]") & 
                                     (df["selftext"]!="[deleted]"))).distinct().select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"ColoradoAvalanche"
    df = root_df.where(root_df["subreddit"] == "ColoradoAvalanche")

    final = (df.where(df["title"].startswith("Avs") & 
                      df["title"].contains("Thread") & 
                      (~df["title"].contains("'")) & 
                      (df["link_flair_text"] == "GDT")
                      ).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"stlouisblues"
    df = root_df.where(root_df["subreddit"] == "stlouisblues")

    final = (df.where(df["title"].startswith("[GDT]") & 
                     (~df["title"].endswith("(2030)")) & 
                     df["author"].startswith("stlouisbluesmods") & 
                     (df["link_flair_text"]!="POSTPONED") &
                     (df["num_comments"]!=0)).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"goldenknights"
    df = root_df.where(root_df["subreddit"] == "goldenknights")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"AnaheimDucks"
    df = root_df.where(root_df["subreddit"] == "AnaheimDucks")

    final = (df.where(((df["title"].startswith("Game Thread - ")) | 
                       (df["title"].startswith("Game Thread:"))) & 
                      (df["selftext"]!="[removed]") & 
                      (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"winnipegjets"
    df = root_df.where(root_df["subreddit"] == "winnipegjets")

    final = (df.where(df["title"].startswith("GDT") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"Predators"
    df = root_df.where(root_df["subreddit"] == "Predators")

    final = (df.where(df["title"].startswith("Game Thread:") &
                     (df["link_flair_text"]=="Game Thread") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"canes"
    df = root_df.where(root_df["subreddit"] == "canes")

    final = (df.where(((df["title"].startswith("GDT:")) | 
                       (df["title"].startswith("[GDT]"))) & 
                      (df["selftext"]!="[removed]") & 
                      (df["selftext"]!="[deleted]") & 
                      (df["num_comments"]!=0))).select("title","num_comments", "created_utc", "id", "subreddit")

    final.write.parquet(output, mode='append')

    #"sabres"
    df = root_df.where(root_df["subreddit"] == "sabres")

    final = (df.where(((df["title"].startswith("Game Thread:")) | 
                       (df["title"].startswith("Game Day Thread"))) & 
                      (df["selftext"]!="[removed]") & 
                      (df["selftext"]!="[deleted]"))).select("title","num_comments", "created_utc", "id", "subreddit")

    final.write.parquet(output, mode='append')

    #"CalgaryFlames"
    df = root_df.where(root_df["subreddit"] == "CalgaryFlames")

    final = (df.where(df["title"].startswith("Game Thread:") &
                     (df["link_flair_text"]=="Game Thread") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"losangeleskings"
    df = root_df.where(root_df["subreddit"] == "losangeleskings")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"OttawaSenators"
    df = root_df.where(root_df["subreddit"] == "OttawaSenators")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"DallasStars"
    df = root_df.where(root_df["subreddit"] == "DallasStars")

    final = (df.where(df["title"].startswith("[Game Thread]") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"Coyotes"
    df = root_df.where(root_df["subreddit"] == "Coyotes")

    final = (df.where(df["title"].startswith("GDT:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"FloridaPanthers"
    df = root_df.where(root_df["subreddit"] == "FloridaPanthers")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')

    #"SeattleKraken" - NEW TEAM IN 2021-2022 Season
    df = root_df.where(root_df["subreddit"] == "SeattleKraken")

    final = (df.where(df["title"].startswith("Game Thread:") &  
                     (df["selftext"]!="[removed]") & 
                     (df["selftext"]!="[deleted]")).select("title","num_comments", "created_utc", "id", "subreddit"))

    final.write.parquet(output, mode='append')
    #saved to parquet, will join with comments in another script

if __name__ == '__main__':
    inputs = "./nhl_subreddit_submissions"
    output = "./nhl_gamethreads_submissions"
    spark = SparkSession.builder.appName('filter game threads').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
