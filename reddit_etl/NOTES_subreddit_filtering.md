## Game threads by subreddit

Each subreddit has different rules and formats for their game threads, so I may need to create seperate sripts for each thread. I am going to record how each subreddit classifies their game threads here and see how I should tackle seperating the game threads from other comment threads. 

Also, in each subreddit there are pre-game discussion threads, post-game, prospective player game threads.. It will not be as simple as filtering by the flair-text. 

- I am focused on regular season games
- We could repartition by the timstamp in the title?
- The teams lower down on the list start to have less acitivty that higher teams on the list
- Look into filtering all byt selftext as I caught some duplicates
    - found both "[removed]" and "[deleted]

- some teams have low following/not much activity (in 2020).
    - TampaBayLightning - 0 threads
    - OttawaSenators - 0 threads
    - AnaheimDucks < 100
    - losangeleskings < 100
    - FloridaPanthers < 100

## ex code to filter

Code to filter example

root_df = spark.read.parquet("./nhl_subreddit_submissions")
df = root_df.where(root_df["subreddit"] == "DetroitRedWings")

# New Subreddit Notes. Using both 2020 and 2021

"hockey"
    - "neutral thread for every game"
    - good - great activity, filter by title and link_flair_text.
    - filter out nwhl games

    Game Thread: Tampa Bay Lightning (40-16-5) at Arizona Coyotes (30-26-8) - 22 Feb 2020 - 06:00PM MST

    final = (df.where(df["title"].startswith("Game Thread:") & 
        (df["link_flair_text"].startswith("[GDT]")) & 
        (~df["title"].contains("NWHL")) & 
        (df["selftext"]!="[removed]") & 
        (df["selftext"]!="[deleted]")))

"leafs"
    - Game threads start with "Game Thread:"
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Toronto Maple Leafs (36-25-9) at Columbus Blue Jackets (33-22-15) - 07 Aug 2020 - 08:00PM EDT

"hawks"
    - Game threads start with "Game Thread:", also has "GDT" link_flair_text but that includes non-regular season games.
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Chicago Blackhawks (27-26-8) at Dallas Stars (35-20-6) - 23 Feb 2020 - 02:00PM CST

"DetroitRedWings"
    - Game threads start with "Game Thread:"
    - CHANGE THIS TO A LIST .isin
    - df.where(df["title"].startswith("Game Thread:") & ~df["title"].startswith("Game Thread: 2021 NHL Draft Lottery") & (df["selftext"]!="[deleted]") & (df["selftext"]!="[removed]")) 

    Game Thread: Montréal Canadiens (27-26-8) at Detroit Red Wings (14-43-4) - 18 Feb 2020 - 07:30PM EST 

"penguins"
    - Game threads start with "Game Thread:"
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Pittsburgh Penguins (29-13-5) at Detroit Red Wings (12-32-3) - 17 Jan 2020 - 07:30PM EST

"canucks"
    - Title starts with GT: and tagged with link_flair_text "GAME THREAD".
    - df.where((df['link_flair_text'] == "GAME THREAD") & (df["title"].startswith("GT:")))

    GT: VANCOUVER CANUCKS @ NEW YORK ISLANDERS - 02/01/2020 - 10:00 AM
    GT: RANGERS @ CANUCKS
        - only two games in this second format

"BostonBruins"
    - Title starts with "GDT:", also link_flair_text as "Game Thread" but contains some irrelevant threads
    - a few duplicate titles which can be removed by filtering selftext != "[deleted]"
    - df.where((df["title"].startswith("GDT:")) & (df["selftext"]!="[deleted]")).select("Title")

    GDT: 3/5 Boston Bruins (42-13-12) @ Florida Panthers (33-26-7) - 7:00 PM ET
    GDT: Boston Bruins vs Washington Capitals
        - These dont have the timestamp in their name.

"Flyers"
    - Title starts with "Game Thread:". No timestamp in title.
    - df.where(df["title"].startswith("Game Thread:")).select("Title")

    Game Thread: New Jersey Devils @ Philadelphia Flyers 7:00pm

"Habs"
    - Title begins with "Game Thread:" or "[GDT]". Has link flair text. Not reliable timestamp
    - df.where((df["title"].startswith("Game Thread:")) | (df["title"].startswith("[GDT]"))).select("Title", "num_comments")

    Game Thread: Montreal Canadiens (20-21-7) at Philadelphia Flyers (25-16-6) - 16 Jan 2020 - 07:00PM EST  
    [GDT] Montréal Canadiens vs Carolina Hurricanes - 29 Feb 2020 - 7:00 PM EST 

"SanJoseSharks"
    - Title startwith "Game Thread:", good timestamp, no link_flair_text.
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: SHARKS (25-28-4) at WILD (27-23-7) - 15 Feb 2020 - 02:00pm PST  

"caps"
    - Title starts with "Game Thread:", link_flair text of GDT is really messy. Good timestamp in title.
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: Carolina Hurricanes (27-16-2) at Washington Capitals (30-11-5) - 13 Jan 2020 - 07:00PM EST

"NewYorkIslanders"
    - Title starts with "Game Thread:"
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: Montréal Canadiens (30-28-9) @ New York Islanders (35-21-8) 07:00 PM 

"wildhockey" (low activity)
    - Title starts with "Game Thread:" or "GDT:", very low comment number compared with other subreddits.
    - df.where(df["title"].startswith("Game Thread:") | df["title"].startswith("GDT:"))
    - recently (2021) they have adopted the title starts with "Game Thread: " strategy.

    GDT: Preds @ the X 

"rangers"
    - Title starts with "Game Thread:". Good timestamp.
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: New York Rangers (33-24-4) at New York Islanders (35-20-6) - 25 Feb 2020 - 07:00PM EST

"devils"
    - Title start with "Game Thread" (no semi-colon). Very good timestamp.
    - df.where(df["title"].startswith("Game Thread")).select("Title", "num_comments")
    
    Game Thread (2020/01/07): New York Islanders (26-12-3) @ New Jersey Devils (15-20-6) — 7:00 PM EST (MSG, MSG+, NHL.TV)  

"BlueJackets"
    - Great data. Title starts with "Game Thread:". Good timestamp.
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: Blue Jackets @ Oilers - March 7th, 2020 | 10pm  

"TampaBayLightning" (No good data)
    -

"EdmontonOilers"
    - Title starts with "Game Day Talk". Good timestamp.
    - df.where(df["title"].startswith("Game Day Talk"))

    Game Day Talk | Oilers v. Blackhawks | 5 March 2020  

"ColoradoAvalanche"
    - Title starts with "avs", contains "Thread", doesnt contain "'".
    - df.where(df["title"].startswith("Avs") & df["title"].contains("Thread") & (~df["title"].contains("'")))

    Avs VS Flyers Streamables Thread (Break Too Long Edition)  

"stlouisblues"
    - Title starts with "[GDT]", doesnt end in "(2030)", and author startswith "stlouisbluesmods". Good timestamp and data
    - df.where(df["title"].startswith("[GDT]") & (~df["title"].endswith("(2030)")) & df["author"].startswith("stlouisbluesmods"))

    [GDT] Game 68: Blues @ Devils - 06-Mar (18:00)   

"goldenknights"
    - Title start with "Game Thread:". Good timestamp. Great Data. (came into league in 2020)
    - df.where(df["title"].startswith("Game Thread:")).select("Title", "num_comments")

    Game Thread: St. Louis Blues (32-15-9) at Vegas Golden Knights (28-22-8)- 13 February 2020 7:00 PST  

"AnaheimDucks"
    - adopted starts with "Game Thread:" in 2021.
    - < 2020 title starts with "Game Thread - ". Low comment activity 50-100 comments per game.
    - df.where(df["title"].startswith("Game Thread - ")).select("Title", "num_comments")

    Game Thread - Edmonton Oilers @ Anaheim Ducks

"winnipegjets"
    - Title starts with "GDT". 100-500 comments per game. need to filter selftext = "[deleted]"
    - df.where(df["title"].startswith("GDT") & (df["selftext"]!="[deleted]"))

    GDT Jets @ Blue Jackets | Wed Jan 22, 2020 6:30pm CT/2:30am EEST      

"Predators"
    - Title starts with game thread. Low activity 50-200 but good data.
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Anaheim Ducks (17-24-5) at Nashville Predators (21-17-7) - 16 Jan 2020 - 07:00PM CST  

"canes"
    - Title starts with "GDT:" or "[GDT]". Good data, good number of comments.
    - df.where(df["title"].startswith("GDT:") | df["title"].startswith("[GDT]"))

    GDT: Hurricanes vs. Blue Jackets (7:10pm)   
    [GDT] Canes vs Rangers 2-21-2020 (7:10 PM)

"sabres"
    - Title starts with "Game Thread:" or "Game Day Thread"
    - df.where(df["title"].startswith("Game Thread:") | df["title"].startswith("Game Day Thread"))
   
    Game Day Thread - 23 February 2020 - Winnipeg Jets (32-26-5) @ Buffalo Sabres (28-25-8) - KeyBank Center 3 PM
    Game Thread: Buffalo Sabres (30-31-8) at Montreal Canadiens (31-31-9) - 12 Mar 2020 - 07:00PM EDT

"CalgaryFlames"
    - Title starts with "Game Thread:". Great data. 
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Calgary Flames (27-22-6) @ Vancouver Canucks (30-20-5)

"losangeleskings"
    - Title starts with "Game Thread:". Low comment activity < 100
    - df.where(df["title"].startswith("Game Thread:"))
    
    Game Thread: Philadelphia Flyers (22-12-5) at Los Angeles Kings (16-21-4) - 31 Dec 2019 - 06:00PM PST

"OttawaSenators"
    - No game threads
    - Started "Game Thread:" trend in 2021.

"DallasStars"
    - Title starts with "[Game Thread]". Filter by not deleted Great data.
    - df.where(df["title"].startswith("[Game Thread]") & (df["selftext"]!="[deleted]"))

    [Game Thread] Dallas Stars @ St. Louis Blues (Feb. 29, 2020)    

"Coyotes"
    - Title starts with "GDT:", filter deleted. Decent data considering how bad the team is
    - df.where(df["title"].startswith("GDT:") & (df["selftext"]!="[deleted]") )

    GDT: Arizona Coyotes (30-24-8) at Dallas Stars (34-19-6)    

"FloridaPanthers"
    - Title starts with "Game Thread:". Low comment activity but good data.
    - df.where(df["title"].startswith("Game Thread:"))

    Game Thread: Florida Panthers (32-24-6) at Arizona Coyotes (31-26-8) - 25 Feb 2020 - 09:00PM EST