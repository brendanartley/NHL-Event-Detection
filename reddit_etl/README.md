## Reddit ETL

The sheer size of the reddit comments corpus is extremely large, and increasing at a fast pace. To filter through the entire dataset to find game threads for each teams fans required a large amount of ETL work. Each thread classified their game threads differently and at different times, so this proved to be quite a challenge. 

There are two other notes files that contain components that aided in the ETL process.

The scripts should be run in the following order. Note that the starting point is the reddit comments corpus which was stored on the hadoop file system on a cluster.

### Script Order

nhl_comments.py 
- input: reddit comments data
- filters reddit comments by those that are in NHL subreddits

nhl_subreddits.py 
- input: reddit submissions data
- filters reddit submissions to those in NHL subreddits

filter_game_threads.py 
- input: output from nhl_subreddits
- filters the game threads from posts

join_subs_and_comms.py
- input: output from nhl_comments.py, output from filter_game_threads.py
- joins relevant comments and relevant subreddits

join_nhlapi_and_reddit.py
- input: output from join_subs_and_comms.py, NHL_API data
- joins the resulting reddit data with the game statistics from the NHL_api

adding_features.py - adds feature if the comment occured 60 seconds after a goal
- input: output from join_nhlapi_and_reddit.py
- adds column that indicate if there has been a goal in the 60 seconds prior to the comment


### NHL Teams Subreddit Data

Each NHL team has a subreddit specific to that team. During each game a bot creates a game thread where people comment throughout the game. We will have to do some sort of join to connect comments to their posts to find comments that are made on each game thread.

We can find a ton of reddit data to play with on the SFU cluster

- /courses/datasets/reddit_comments_repartitioned/year=2020
- /courses/datasets/reddit_submissions_repartitioned/year=2020
    - There is also a readme in here.

- Note we can scale up to include more years, but there is definatley more activity in more recent years in game threads for the NHL.

### nhl_comment_sample

In the /nhl_sample directory, I sampled (year=2019 month=11) and filtered only comments from NHL subreddits to get a sense of the data.

- We can also get data directly from a reddit comment thread url by simply adding .json at the end.

### Submissions Data

Submissions data refers to each post.

Game threads in each NHL teams subreddits have a format like the following.

    "Game Thread: Washington Capitals (21-4-5) at Anaheim Ducks (12-12-4) - 06 Dec 2019 - 10:00PM EST"

There can also be playoff game threads that seem to be the most active thread type. There also seems to be a pre-season game thread, but these are new to the last few years so there might be limited data on these.

    "Playoff Game Thread: Game 7 - Carolina Hurricanes (3 - 3) at Washington Capitals(3 - 3) - 24 Apr 2019 - 07:30PM EDT"

### Comments Data

Comments data refers to the comments made on every post.

link_id - id of the post that the comment is replying to.
gilded - has the comment been given any reddit gold.

### NHL subreddit names

    nhl_subs = [
            "hockey",
            "leafs",
            "hawks",
            "DetroitRedWings",
            "penguins",
            "canucks",
            "BostonBruins",
            "Flyers",
            "Habs",
            "SanJoseSharks",
            "caps",
            "NewYorkIslanders",
            "wildhockey",
            "rangers",
            "devils",
            "BlueJackets",
            "TampaBayLightning",
            "EdmontonOilers",
            "ColoradoAvalanche",
            "stlouisblues",
            "goldenknights",
            "AnaheimDucks",
            "winnipegjets",
            "Predators",
            "canes",
            "sabres",
            "CalgaryFlames",
            "losangeleskings",
            "OttawaSenators",
            "DallasStars",
            "Coyotes",
            "FloridaPanthers"]

