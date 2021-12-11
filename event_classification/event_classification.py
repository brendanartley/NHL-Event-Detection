import sys
import sparknlp
from pyspark.sql import SparkSession, functions
from pyspark.sql import types
from sparknlp.annotator import Lemmatizer, Stemmer, Tokenizer, Normalizer, LemmatizerModel
from sparknlp.base import DocumentAssembler, Finisher
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover

# Define lexicons
goals = ['goal','goals']
penalties = ['penalty','penalties']

def unix2utc(df,col):
    df_utc = df.withColumn('mid_window_utc',functions.to_timestamp(df[col]))
    return df_utc

def main(input1, input2, output):
    comments = spark.read.parquet(input1)
    comments_useful = comments.filter(comments['is_useful']==True)
    comments_useful_utc = unix2utc(comments_useful,'mid_window')
    
    #spark-nlp pipeline
    document_assembler = DocumentAssembler().setInputCol("body").setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalizer").setLowercase(True)
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

    #classification by lexicons UDF
    goal_lexicons = ["goal", "score", "lead", "net", "assist", "play", "pass"] + ["good", "go", "lol", "one"]
    def check_lexicons(arr, lexicons):
        for val in arr:
            if val in lexicons:
                return 1
        return 0
    lexicon_count = functions.udf(lambda x: check_lexicons(x, goal_lexicons), types.IntegerType())

    #Get predictions
    df = comments_processed.where(comments_processed["valid_window_size"] == 60)
    with_lex_count = df.withColumn("lex_count", lexicon_count(df["filtered"]))
    grouped_lex_count = with_lex_count.groupBy("game_id", "mid_window").agg(functions.count("lex_count").alias("lex_count_sum"), functions.avg("lex_count").alias("lex_count_avg"))
   
   # grouped_lex_count.write.parquet("glc_sample", mode="overwrite")

    grouped_lex_count_filtered = grouped_lex_count.where(grouped_lex_count["lex_count_avg"] > 0.2)
    predictions_df = grouped_lex_count_filtered.groupBy("game_id").agg(functions.collect_list("mid_window").alias("predicted_goal_times"))

    #custom evaluation metric - how accurate are we?
    game_stats = spark.read.parquet(input2)
    goal_stats = game_stats.where(game_stats["event_name"] == "Goal").withColumn("unix_event_datetime", functions.unix_timestamp(functions.to_timestamp(game_stats["event_datetime"])))
    goals_by_game = goal_stats.groupBy("game_id").agg(functions.collect_list("unix_event_datetime").alias("actual_goal_times"))

    #  --- UDF Functions for clustering predictions + Evaluating Performance --- 
    def cluster_predictions(preds):
        """
        This algorithm is a clustering method to reduce
        the number of windows that predict the same goal.
        """

        if len(preds) < 2:
            return preds

        s = sorted(preds)
        final = []
        last = s[0]
        streak = False
        start = last

        for pred in s[1:]:
            if pred - last < 60:
                if streak == True:
                    last = pred
                else:
                    start = last
                    last = pred
                    streak=True
            else:
                if streak == True:
                    print("here")
                    final.append((start+last)//2)
                    start = pred
                    last = pred
                    streak = False
                else:
                    final.append(last)
                    last = pred

        if streak == True:
            final.append((start+last)//2)
        return final
    cluster_predictions = functions.udf(cluster_predictions, types.ArrayType(types.LongType())) 

    def prediction_acc(predictions, actuals):
        """
        returns percentage (as float) of predicted events that
        are within 60 seconds of a goal
        """
        c=0
        for pred in predictions:
            for act in actuals:
                if abs(act - pred) <= 180:
                    c+=1
                    break
        return c/len(predictions)
    predictions_correct = functions.udf(prediction_acc, types.FloatType()) 

    #what percentage of goals were predicted?
    def goals_predicted(predictions, actuals):
        """
        returns the percentage of actual that were
        within 60 seconds of a prediction
        """
        c = len(actuals) * [0]
        for pred in predictions:
            for i in range(len(actuals)):
                if abs(actuals[i] - pred) <= 180:
                    c[i] = 1
                    break
        return sum(c) / len(c)
    goals_correct = functions.udf(goals_predicted, types.FloatType()) 
  
    #Joining game_stats + clustering predictions
    joined_df = predictions_df.join(goals_by_game, on="game_id")
    joined_df = joined_df.withColumn("predicted_goal_times", cluster_predictions(joined_df["predicted_goal_times"]))

    with_metrics = joined_df.select("*", predictions_correct(joined_df["predicted_goal_times"], joined_df["actual_goal_times"]).alias("acc1"), goals_correct(joined_df["predicted_goal_times"], joined_df["actual_goal_times"]).alias("acc2"))
    fm = with_metrics.agg(functions.avg(functions.col("acc1"))).collect()[0][0]
    sm = with_metrics.agg(functions.avg(functions.col("acc2"))).collect()[0][0]

    with_metrics.show()
    with_metrics.write.parquet(output, mode="overwrite")

    with open("results.txt", "w") as text_file:
        text_file.write("\nPercent of predictions within 60 seconds of goal: {} \n".format(fm))
        text_file.write("Percent of actual goals within 60 seconds of prediction: {} \n".format(sm))

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    output = sys.argv[3]
    spark = sparknlp.start()
    # spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # assert spark.version >= '3.0'
    # spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1, input2, output)