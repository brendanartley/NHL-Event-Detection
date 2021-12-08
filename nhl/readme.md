There are three useful functions in this package

1. crawl_stats_in_year()
    It crawls game stats from NHL API in a given year
2. get_single_game_stats()
    It crawls game stats from NHL API for a given game
3. collect_game_ids()
    A helper function. It collects game ids in the "in a year" file into a python list.

Read the doc string for more info about them.

There is a useful script `convert_linescore_to_parquet` to convert json file into parquet file.
It takes two command line arguments: json file path and output folder name. The output will be stored in gamestats/parquet/output_folder_name