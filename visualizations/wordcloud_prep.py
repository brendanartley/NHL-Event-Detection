import sys
import sparknlp
from pyspark.sql import SparkSession, functions, types
from sparknlp.annotator import Lemmatizer, Stemmer, Tokenizer, Normalizer, LemmatizerModel
from sparknlp.base import DocumentAssembler, Finisher
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover

def main(inputs, output):

    #read df
    df = spark.read.parquet(inputs)
    df_useful = df.filter(df['recent_goal']==1)
    df_not_useful = df.filter(df['recent_goal']==0)
    
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
    
    #fit pipeline to both datasets
    model = pipeline.fit(df_useful)
    useful_df_processed = model.transform(df_useful)
    not_useful_df_processed = model.transform(df_not_useful)


    #writing most frequent words to output
    useful_final = (useful_df_processed.withColumn('word', functions.explode(useful_df_processed["filtered"]))\
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
    .limit(100)
    .withColumn("useful", functions.lit(1)))

    not_useful_final = (not_useful_df_processed.withColumn('word', functions.explode(not_useful_df_processed["filtered"]))\
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
    .limit(100)
    .withColumn("useful", functions.lit(0)))

    #tableau by default doesnt support parquet + needs one csv file 
    #always <200 rows, so coalesce is fine here
    useful_final.union(not_useful_final).coalesce(1).write.csv(output, mode="overwrite")

if __name__ == '__main__':
    inputs = "nhl_comments_final_v2"
    output = "wordcount_output"
    spark = sparknlp.start()
    # spark = SparkSession.builder.appName('window').getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # assert spark.version >= '3.0'
    # spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)