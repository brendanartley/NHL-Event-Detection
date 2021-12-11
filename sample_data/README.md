## Sample Data

Given the sheer size of the data that we are working with, we used data stored in the Hadoop file system on SFU's compute cluster and also using the Vault. The files were simply too large to store in a git repo. 

We have provided a few samples here to give a sense of some of the data we worked with.

### Reddit Sample

This sample is of submissions and comments found in NHL subreddits. These two samples are the outputs from nhl_comments.py and nhl_subreddits.py in the ETL process. 

### Twitter Sample

For the twitter sample, we provided the schema that is returned by the scraper that was implemented to retrieve tweets during games. The raw data is returned in JSON format. 

### GameStats Sample

The game stats sample contains both a sample of linescore and live-feed data. Each sample shows unique columns that were needed in identifying the start time and end time of games, and also events that occurred throughout the games.