import sys
import sparknlp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from sparknlp.annotator import Lemmatizer, Stemmer, Tokenizer, Normalizer, LemmatizerModel
from sparknlp.base import DocumentAssembler, Finisher
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover

# Define lexicons
goals = ['goal','goals']
penalties = ['penalty','penalties']

LECXICON_THRES = 4

def unix2utc(df,col):
    df_utc = df.withColumn('mid_window_utc',F.to_timestamp(df[col]))
    return df_utc

def main(inputs, output):
    comments_path, events_path = inputs
    comments = spark.read.parquet(comments_path)
    comments_useful = comments.filter(comments['is_useful']==True)
    comments_useful_utc = unix2utc(comments_useful,'mid_window')
    
    #spark-nlp pipeline
    document_assembler = DocumentAssembler().setInputCol("body").setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalizer").setLowercase(True).setCleanupPatterns(["""[^\w\d\s]"""])
    lemmatizer = LemmatizerModel.pretrained('lemma_antbnc', 'en').setInputCols(["normalizer"]).setOutputCol("lemma")
    finisher = Finisher().setInputCols(["lemma"]).setOutputCols(["to_spark"]).setValueSplitSymbol(" ")

    stopword_remover = StopWordsRemover(inputCol="to_spark", outputCol="filtered")

    pipeline = Pipeline(
    stages = [
        document_assembler,
        tokenizer,
        normalizer,
        lemmatizer,
        finisher,
        stopword_remover])
    
    model = pipeline.fit(comments_useful_utc)
    comments_processed = model.transform(comments_useful_utc)

    goal_lexicons = ["goal", "score", "lead", "net", "assist", "play", "pass", "fuck", "shot", "shit", "miss"]
    comments_with_lexicons = comments_processed.withColumn("goal_lexicons", F.array([F.lit(lexicon) for lexicon in goal_lexicons]))
    comments_with_lexicons_count = comments_with_lexicons.withColumn("goal_lexicons_count", 
                                                                    F.size(F.array_intersect("filtered", "goal_lexicons")))

    # Get predictions
    comments_with_features = comments_with_lexicons_count.groupBy("game_id", "mid_window").\
                                            agg(F.sum("goal_lexicons_count").alias("total_goal_lex_count"),
                                                F.collect_list("filtered").alias("comment_summary"))
    predicted = comments_with_features.where(F.col("total_goal_lex_count") > LECXICON_THRES)
    
    # Calculate accuracy
    game_stats = spark.read.parquet(events_path)
    goal_stats = game_stats.where(game_stats["event_name"] == "Goal").\
                        withColumn("event_unix", F.unix_timestamp(game_stats["event_datetime"], "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    total_goals = goal_stats.join(predicted, on="game_id").\
                            select(F.col("event_unix")).\
                            distinct().\
                            count()

    join_cond = [predicted["game_id"] == goal_stats["game_id"],
                predicted["mid_window"] - goal_stats["event_unix"] > 0,
                predicted["mid_window"] - goal_stats["event_unix"] < 60]
    pred_with_truth = predicted.join(goal_stats, on=join_cond, how="left_outer").\
                                select(predicted["*"], goal_stats["event_unix"]).\
                                groupBy(predicted["game_id"], predicted["mid_window"]).\
                                agg(F.max(goal_stats["event_unix"]).alias("event_unix"),
                                    F.first(predicted["comment_summary"])).cache()
    
    correct = pred_with_truth.where(F.col("event_unix").isNotNull()) 
    correct.show(30, False, True)
    correct_count = correct.count()

    total = pred_with_truth.count()
    print(f"Total goals: {total_goals}")
    print(f"Total predicted goals: {total}")
    print(f"Total correct predictions: {correct_count}")
    print(f"Accuracy: {(correct_count / total):.4f}")

if __name__ == '__main__':
    inputs = [sys.argv[1], sys.argv[2]]
    output = sys.argv[3]
    comment_type = sys.argv[4]

    spark = sparknlp.start()

    if comment_type == "reddit":
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    elif comment_type == "twitter":
        pass
    else:
        print("\n ERROR: -- Last argument must be reddit or twitter -- \n")
        sys.exit()

    # spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    #spark.conf.set("spark.sql.session.timeZone", "UTC")
    # assert spark.version >= '3.0'
    # spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
