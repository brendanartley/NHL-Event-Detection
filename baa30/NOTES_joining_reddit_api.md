## Notes on how we are going to join with the NHL API
   
I want to check when the game threads are created. Maybe we can we partition the day from utc tag?
 - I think bofei is right. It automatically converts to the time zone we are in. 
    - need to take this into consideration

# Using Subreddit's created_utc

The subreddits all have different rules as to when they create threads so I had to find a method to get the correct day that they were playing. Since UTC midnight falls around 2pm, this could be problematic when we repartition by date.

I went through all the threads and found that we can create an adjusted created_utc that gives us the exact day that the game was played on, and that matches the API day format. This would make repartitioning the data so much easier.

    #"hockey" - day is good
    #"leafs" - day is good
    #"hawks" - day is good
    #"DetroitRedWings" - day is good
    #"penguins" - day is good
    #"canucks" - day is good
    #"BostonBruins" - day is good
    #"Flyers" - day is good
    #"Habs" - day is good
    #"SanJoseSharks" - day is good
    #"caps" - day is good
    #"NewYorkIslanders" - day is good
    #"wildhockey" - day is good
    #"rangers" - day is good
    #"devils" - day is good
    #"BlueJackets" - + 4hrs (if we dont we miss 5)
    #"TampaBayLightning" - day is good
    #"EdmontonOilers" - day is good
    #"ColoradoAvalanche" - day is good
    #"stlouisblues" - day is good
    #"goldenknights" - day is good
    #"AnaheimDucks" - day is good
    #"winnipegjets" - + 4hrs to timestamp (for all)
    #"Predators" - day is good
    #"canes" - day is good
    #"sabres" - day is good
    #"CalgaryFlames" - day is good
    #"losangeleskings" - day is good
    #"OttawaSenators" - day is good
    #"DallasStars" - +3hrs (if we dont we lost three)
    #"Coyotes" - day is good
    #"FloridaPanthers" - day is good

# Hockey subreddit on partition_by

I think saving in the following file system format would be good.

- Canucks
    - 05/02/2021
        - canucks_subreddit
        - flames_subreddit
        - hockey_subreddit
        - twitter_data

Two formats:
    
    a. "Game Thread: Washington Capitals (37-15-5) at Arizona Coyotes (28-24-8) - 15 Feb 2020 - 08:00PM MST"
    b. "Game Thread: San Jose Sharks at Arizona Coyotes - 14 Jan 2021 - 07:00PM MST"


# Nhl API Names

These are the team names used by the api. We need to join these with the reddit_data somehow.

Florida Panthers
Arizona Coyotes
Los Angeles Kings
Winnipeg Jets
Colorado Avalanche
Minnesota Wild
New York Rangers
Philadelphia Flyers
Vancouver Canucks
Detroit Red Wings
San Jose Sharks
Boston Bruins
Calgary Flames
Chicago Blackhawks
Pittsburgh Penguins
Edmonton Oilers
New Jersey Devils
Toronto Maple Leafs
Columbus Blue Jackets
Ottawa Senators     
Dallas Stars    
Washington Capitals
Montréal Canadiens
St. Louis Blues
Anaheim Ducks 
Nashville Predators
Carolina Hurricanes
Vegas Golden Knights
New York Islanders
Buffalo Sabres
Tampa Bay Lightning

|Florida Panthers     |
|Arizona Coyotes      |
|Los Angeles Kings    |
|Winnipeg Jets        |
|Colorado Avalanche   |
|Minnesota Wild       |
|New York Rangers     |
|Philadelphia Flyers  |
|Vancouver Canucks    |
|Detroit Red Wings    |
|San Jose Sharks      |
|Boston Bruins        |
|Calgary Flames       |
|Montral Canadiens    |
|Chicago Blackhawks   |
|Edmonton Oilers      |
|Pittsburgh Penguins  |
|New Jersey Devils    |
|Toronto Maple Leafs  |
|Columbus Blue Jackets|
|Ottawa Senators      |
|Dallas Stars         |
|Washington Capitals  |
|Anaheim Ducks        |
|St Louis Blues       |
|Montreal Canadiens   |
|Nashville Predators  |
|Carolina Hurricanes  |
|Vegas Golden Knights |
|New York Islanders   |
|Buffalo Sabres       |
|Tampa Bay Lightning  |

