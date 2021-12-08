import json, os
from datetime import datetime
from . import constants as c
from collections import namedtuple

MatchDigest = namedtuple("MatchDigest", ["game_id", "home_team_id", "away_team_id", "start_time", "end_time"])

def extract_match_info(linescore):
    try:
        match_digest = MatchDigest(game_id=linescore["gameID"],
                                home_team_id=linescore["teams"]["home"]["team"]["id"],
                                away_team_id=linescore["teams"]["away"]["team"]["id"],
                                start_time=datetime.fromisoformat(linescore["periods"][0]["startTime"].replace("Z", "+00:00")),
                                end_time=datetime.fromisoformat(linescore["periods"][-1]["endTime"].replace("Z", "+00:00")))
    except (KeyError, IndexError) as e:
        print(f"linescore lacks some attributes, perhaps the game has not been played yet")
    else:
        return match_digest

def load_team_name_dict(teams_file_path):
    with open(teams_file_path, "r") as team_file:
        all_teams_dict = json.load(team_file)
    
    team_by_id = {}
    for team_dict in all_teams_dict["teams"]:
        team_by_id[team_dict["id"]] = (team_dict["name"], team_dict["teamName"], team_dict["abbreviation"])
    return team_by_id
