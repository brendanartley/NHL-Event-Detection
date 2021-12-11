## To Test the Project
Please refer to the specific instructions for each stage stored in the `README.md` file in the respective path.
1. `nhl_api_etl` - generate and clean NHL API game stats
2. `reddit_etl` - generated cleaned and schema-unified reddit dataset
3. `twitter_scraper` - generate raw twitter dataset
4. `twitter_etl` - generate cleaned and schema-unified twitter dataset
5. `event_detection` - detect event in datasets
6. `event_classification` - generate the result dataset

Please refer to `requirements.txt` for all the packages and tools used in this project. When `spark-submit` is run using the configuration `--packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4`, the configuration can be substituted by `--jars spark-nlp-assembly-3.3.4.jar`, in case downloading packages is not possible. The file `spark-nlp-assembly-3.3.4.jar` can be found in `/home/bja39/spark-nlp-assembly-3.3.4.jar` as well as [here](https://github.com/JohnSnowLabs/spark-nlp/releases)

Small sample datasets generated from step 1, 2 and 4 were stored in `sample_data`.
Due to the excessive size of the raw datasets, they are not included in this repo. 

However, the entire raw reddit datasets can be found in HDFS under the path `/courses/datasets/reddit_comments_repartitioned/`, the complete output of step 1 is stored in `/home/bja39/gamestats`, and complete output of step 4 can be found in `/home/bja39/unified_tweets_complete`.