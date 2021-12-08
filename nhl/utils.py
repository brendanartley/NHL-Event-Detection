import json, os
from . import constants as c

def load_team_name_dict(teams_file_path=os.path.join(c.STATS_DIR, c.STATS_TEAMS_FILE_NAME)):
    with open(teams_file_path, "r") as team_file:
        all_teams_dict = json.load(team_file)
    
    team_by_id = {}
    for team_dict in all_teams_dict["teams"]:
        team_by_id[team_dict["id"]] = (team_dict["name"], team_dict["teamName"], team_dict["abbreviation"])
    return team_by_id

def collect_game_ids(year_file_path):
    game_ids = []
    with open(year_file_path, "r") as f:
        for line in f:
            game_ids.append(json.loads(line)["gameID"])
    return game_ids