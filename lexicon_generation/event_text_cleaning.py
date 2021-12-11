import sys
import os
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
    df_utc = df
    return df_utc

def main(inputs, output):
    comments = spark.read.parquet(inputs)
    comments_utc = comments.withColumn('mid_window_utc', F.to_timestamp("mid_window"))
    
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
    
    model = pipeline.fit(comments_utc)
    comments_processed = model.transform(comments_utc)

    # Get predictions
    comments_with_features = comments_processed.groupBy("game_id", "mid_window").\
                                                agg(F.collect_list("filtered").alias("comment_summary"))
    comments_with_features.write.parquet(output, mode="overwrite")

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = sparknlp.start()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    sc = spark.sparkContext
    main(inputs, output)
