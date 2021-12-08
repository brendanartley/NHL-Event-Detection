import asyncio
import json
import os


import aiohttp
import aiofiles
from tqdm.auto import tqdm
import requests

from . import constants as c
from .constants import Season, DetailLevel

def get_single_game_stats(year: int, season: Season, game_number: int, detail_level=DetailLevel.LINESCORE, output_dir=None) -> str:
    '''
    This function crawls game stats for a single game, and store it in a json file in output_dir.

    year: The year
    season: Refer to Season class

    game_number:
    quoted from NHL API doc
        For regular season and preseason games, this ranges from 0001 to the number of games played. 
        (1271 for seasons with 31 teams (2017 and onwards) and 1230 for seasons with 30 teams). 
        For playoff games, the 2nd digit of the specific number gives the round of the playoffs, 
        the 3rd digit specifies the matchup, and the 4th digit specifies the game (out of 7).
    
    detail_level: Refer to DetailLevel class
    output_dir: Where you want your json file
    '''
    if not output_dir:
        output_dir = c.STATS_DEFAULT_RAW_DIR[detail_level]
    os.makedirs(output_dir, exist_ok=True)

    game_id = f"{year}{season.value}{game_number:04}"
    r = requests.get(c.STATS_API_FORMAT.format(id=game_id, stats_type=detail_level.value))
    try:
        with open(os.path.join(output_dir, f"{year}_{season.name}_{game_number}_{detail_level.name}.json"), "w+") as f:
            json.dump(r.json(), f, indent=4)
    except requests.RequestException as e:
        print(e)

def crawl_stats_in_year(year: int, output_dir=None, game_id_list=None):
    '''
    This functions crawls stats for all games in a year.
    year: The year
    detail_level: Refer to DetailLevel class.
    output_dir: Where you want your json file
    gamd_id_list: If you already have a list of game ids, put them there. Otherwise the function would generate all possible game id and try each of them.
    '''
    detail_level=DetailLevel.LINESCORE
    if not output_dir:
        output_dir = c.STATS_DEFAULT_RAW_DIR[detail_level]
    os.makedirs(output_dir, exist_ok=True)
    
    async def _internal():
        if not game_id_list:
            # this is bad, but NHL can handle 5000 requests once in a while.
            all_possible_ids = [f"{year}{season.value}{game_number:04}" for game_number in range(c.STATS_MAX_TOTAL_GAMES) for season in Season]
        else:
            all_possible_ids = game_id_list

        async with aiofiles.open(os.path.join(output_dir, f"match_{detail_level.value}_in_{year}.json"), "w") as f:
            async with aiohttp.ClientSession() as session:
                for i in tqdm(range(len(all_possible_ids))):
                    id = all_possible_ids[i]
                    async with session.get(c.STATS_API_FORMAT.format(id=id, stats_type=detail_level.value)) as resp:
                        try:
                            content = await resp.json()
                            if content.get(c.STATS_API_CODE_KEY) == c.STATS_API_MATCH_NOT_FOUND_CODE:
                                raise Exception(f"game id {id} is invalid")
                            content['gameID'] = id
                        except Exception as e:
                            print(e)
                        else:
                            await f.write(json.dumps(content))
                            await f.write("\n")
    asyncio.run(_internal())

def crawl_events_in_year(year: int, output_dir=None):
    game_id_list = extract_game_ids(os.path.join(c.STATS_DEFAULT_RAW_DIR[DetailLevel.LINESCORE], f"match_{DetailLevel.LINESCORE.value}_in_{year}.json"))
    if not game_id_list:
        game_id_list = [f"{year}{season.value}{game_number:04}" for game_number in range(c.STATS_MAX_TOTAL_GAMES) for season in Season]

    detail_level = DetailLevel.LIVE_FEED
    if not output_dir:
        output_dir = c.STATS_DEFAULT_RAW_DIR[detail_level]
    os.makedirs(output_dir, exist_ok=True)

    async def _internal():
        async with aiohttp.ClientSession() as session:
            for i in tqdm(range(len(game_id_list))):
                id = game_id_list[i]
                async with session.get(c.STATS_API_FORMAT.format(id=id, stats_type=detail_level.value)) as resp:
                    try:
                        content = await resp.json()
                        if content.get(c.STATS_API_CODE_KEY) == c.STATS_API_MATCH_NOT_FOUND_CODE:
                            raise Exception(f"game id {id} is invalid")
                        start, home_team, away_team = content["gameData"]["datetime"]["dateTime"], content["gameData"]["teams"]["home"]["name"].replace(" ", "-"), content["gameData"]["teams"]["away"]["name"].replace(" ", "-")
                    except Exception as e:
                        print(f"game id {id} failed with exception: {e}")
                    else:
                        async with aiofiles.open(os.path.join(output_dir, f"{id}_{home_team}_{away_team}_{start}.json"), "w") as f:
                            await f.write(json.dumps(content, indent=4))
    asyncio.run(_internal())

def extract_game_ids(yearly_linescore):
    id_list = []
    if os.path.isfile(yearly_linescore):
        with open(yearly_linescore, "r") as f:
            for line in f:
                id_list.append(json.loads(line)["gameID"])
    return id_list