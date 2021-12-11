## NHL_API

This NHL_API data was extremely useful in providing the accurate time of events during each NHL game. This was a very comprehensive API and provides much more information than we needed for the project. Given the richness of the data, further exploration of the API is definitely a must.

This package is used for gathering NHL API data.

Usage:
`python -m nhl_api_etl linescore/livefeed year_to_scrape`
Note that the first argument is either `linescore` or `livefeed`; `year_to_scrape` should be an integer like `2021`.

By default, running it in linescore or livefeed mode would result in all data in the specified year be downloaded. However, if there was previously gather linescore in `gamestas/linescore/raw` already, the livefeed mode would only request for the games that are included in the linescore.

Ideally, run the package in linescore mode first, then run it in livefeed mode.

The output will be stored in `gamestats/linescore/raw` and `gamestats/livefeed/raw`, and for the next stage to work, the directory `gamestats` would need to be put into HDFS manually.
`hdfs dfs -put gamestats/`

Scripts `convert_linescore_to_parquet` and `convert_livefeed_to_parquet` are used to convert json files into parquet files.
They take one command line arguments: json file path 
The output will be stored in gamestats/type_of_stats/parquet/

Please run the following commands from the project root directory.
`spark-submit nhl_api_etl/convert_linescore_to_parquet.py gamestats/linescore/raw`
`spark-submit nhl_api_etl/convert_livefeed_to_parquet.py gamestats/livefeed/raw`

The output will be stored in `gamestats/linescore/parquet` and `gamestats/livefeed/parquet`