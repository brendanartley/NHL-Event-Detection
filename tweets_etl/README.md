## Twitter ETL

The Twitter ETL process required some manipulation as well to engineer the data into a common format with the Reddit data and NHL API. The hardest part of this step was ensuring that the data was all in the same timezone and format. Spark has a tendency to convert the timestamp type to whatever time its operating system was in, and this was troublesome for us throughout the process. 

There are two scripts used here, one to clean the data and the other to "enhance" the data into a common form with the Reddit data.

### Usage
To apply the ETL, please run the following commands from the project root directory.
`spark-submit tweets_etl/tweet_cleaning.py twitter_data/raw gamestats/linescore/parquet cleaned_tweets`
`spark-submit tweets_etl/tweet_enhancing.py cleaned_tweets gamestats/livefeed/parquet unified_tweets`

The output would be cleaned tweets in parquet format, with the same schema as the reddit dataset.
