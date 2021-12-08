## NHL Teams Subreddit Data

Each NHL team has a subreddit specific to that team. During each game a bot creates a game thread where people comment throughout the game. We will have to do some sort of join to connect comments to their posts to find comments that are made on each game thread.

We can find a ton of reddit data to play with on the SFU cluster

- /courses/datasets/reddit_comments_repartitioned/year=2020
- /courses/datasets/reddit_submissions_repartitioned/year=2020
    - There is also a readme in here.

- Note we can scale up to include more years, but there is definatley more activity in more recent years in game threads for the NHL.

### Script Definitions

nhl_comments.py - filters all of reddit comments to those in NHL subreddits
nhl_subreddits.py - filters all of reddit submissions to those in NHL subreddits

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

