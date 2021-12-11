import os 
from enum import Enum

import findspark
findspark.init()
from pyspark.sql import types

class Season(Enum):
    PRESEASON = "01"
    REGULAR_SEASON = "02"
    PLAYOFF = "03"
    ALL_STAR = "04"

class DetailLevel(Enum):
    ''' 
    All data about a specified game id including play data with on-ice coordinates and post-game
    details like first, second and third stars and any details about shootouts.
    The data returned is simply too large at often over 30k lines and is best explored with a JSON viewer
    '''
    LIVE_FEED = "feed/live"

    '''less detail than live feed'''
    BOXSCORE = "boxscore"

    '''
    even less detail than boxscore 
    Has goals, shots on goal, powerplay and goalie pulled status, 
    number of skaters and shootout information if applicable
    '''
    LINESCORE = "linescore"

STATS_DIR = "gamestats"
STATS_PARQUET_DIR = "parquet"
STATS_RAW_DIR = "raw"
STATS_DEFAULT_RAW_DIR = {
    DetailLevel.LINESCORE: os.path.join(STATS_DIR, "linescore", STATS_RAW_DIR),
    DetailLevel.LIVE_FEED: os.path.join(STATS_DIR, "livefeed", STATS_RAW_DIR),
    DetailLevel.BOXSCORE: os.path.join(STATS_DIR, "boxscore", STATS_RAW_DIR)
}
STATS_DEFAULT_PARQUET_DIR = {
    DetailLevel.LINESCORE: os.path.join(STATS_DIR, "linescore", STATS_PARQUET_DIR),
    DetailLevel.LIVE_FEED: os.path.join(STATS_DIR, "livefeed", STATS_PARQUET_DIR),
    DetailLevel.BOXSCORE: os.path.join(STATS_DIR, "boxscore", STATS_PARQUET_DIR) 
}

STATS_TEAMS_FILE_NAME = "all_teams.json"
STATS_API_FORMAT = "https://statsapi.web.nhl.com/api/v1/game/{id}/{stats_type}"

# as per https://gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md#game-ids
STATS_MAX_TOTAL_GAMES = 1271

# trial and error
STATS_API_CODE_KEY = "messageNumber"
STATS_API_MATCH_NOT_FOUND_CODE = 2

STATS_LINESCORE_SCHEMA = types.StructType([
    types.StructField('game_id', types.StringType()),
    types.StructField('home_team_id', types.StringType()),
    types.StructField('home_team_name', types.StringType()),
    types.StructField('away_team_id', types.StringType()),
    types.StructField('away_team_name', types.StringType()),
    types.StructField('start_time', types.StringType()),
    types.StructField('end_time', types.StringType())
])