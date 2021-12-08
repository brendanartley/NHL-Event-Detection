from datetime import datetime, timedelta
import concurrent.futures
import os
import json
import pathlib

import twint

from . import utils
from . import constants as c

def get_tweets_by_keywords(keywords: list, start_time: datetime=None, end_time: datetime=None, 
                            filename="tweets", simple_output=False):
    config = twint.Config()
    config.Lang = "en"

    if start_time:
        config.Since = (start_time - timedelta(days=1)).strftime("%Y-%m-%d")
    if end_time:
        end_with_margin = end_time + timedelta(days=2)
        config.Until = end_with_margin.strftime("%Y-%m-%d")
    if simple_output:
        config.Custom["tweet"] = ["id", "username", "created_at", "timezone", "tweet"]

    output_path = f"{c.TWITTER_DIR}/{c.TWITTER_RAW_DIR}/{filename}.json"
    previous_finished = f"{c.TWITTER_DIR}/{c.TWITTER_RAW_DIR}/Finished_{filename}.json"
    if os.path.isfile(previous_finished):
        print(f"{filename} already finished, skipping")
        return
    if os.path.isfile(output_path):
        os.remove(output_path)

    config.Output = output_path
    config.Query = " OR ".join(keywords)
    config.Store_json = True
    config.Hide_output = True
    twint.run.Search(config)

    return output_path

def crawl_all(line_score_file, team_names_file, regular_season_only=True):
    team_name_dict = utils.load_team_name_dict(team_names_file)
    kwarg_list = []
    with open(line_score_file, "r") as f:
        for line in f:
            kwarg = {}
            digest = utils.extract_match_info(json.loads(line))
            if not digest or (regular_season_only and digest.game_id[4:6] != "02"):
                continue
            home_team_names = ['"'+ word + '"' for word in team_name_dict[digest.home_team_id][:2]]
            away_team_names = ['"'+ word + '"' for word in team_name_dict[digest.away_team_id][:2]]
            kwarg['keywords'] = home_team_names + away_team_names
            kwarg['filename'] = f"{team_name_dict[digest.home_team_id][0]}_v_{team_name_dict[digest.away_team_id][0]}_{digest.start_time.strftime('%Y-%m-%d')}_{digest.game_id}".replace(" ", "_")
            kwarg['start_time'] = digest.start_time
            kwarg['end_time'] = digest.end_time
            kwarg_list.append(kwarg)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=c.TWITTER_MAX_THREADS) as executor:
        future_dict = {executor.submit(get_tweets_by_keywords, **kwarg): kwarg["filename"] for kwarg in kwarg_list}
        for future in future_dict:
            output_filename = future_dict[future]
            try:
                output_filepath = future.result(timeout=1800)
                if output_filepath:
                    output_path_obj = pathlib.Path(output_filepath)
                    output_path_obj.rename(pathlib.Path(output_path_obj.parent, f"Finished_{output_path_obj.stem}{output_path_obj.suffix}"))
            except Exception as e:
                print(f"Error while crawling for {output_filename}: {e}")
            else:
                print(f"Finished crawling {output_filename}")
    
if __name__ == "__main__":
    crawl_all("gamestats/raw/match_linescore_in_2021.json", "gamestats/all_teams.json")
    #get_tweets_by_keywords(['"Vegas Golden Knights"', '"Golden Knights"', '"Seattle Kraken"', '"Kraken"'], datetime(2021, 11, 2), datetime(2021, 11, 5))