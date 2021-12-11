## Visualizations ETL

Given that we are working with a large amount of data, we had to do even more filtering and aggregating to make the visualizations possible. It was doable using all the data, but doing so in an interactive way was not doable with large amounts of data. 

Once we simplified the data into something more manageable we were able to create some pretty good visualizations.


## Scripts

wordcloud_prep.py

-  code to aggregate the data and create a frequency count of words found in comments after goals and for all other comments. 
-  output is a CSV file of 200 rows. 100 for comments after goals, and 100 for other comments
-  we used this to create a wordplot in Tableau.

spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4  wordcloud_prep.py


gamestats_converter.py

-  Code to convert NHL gamestats data into a timeline-friendly format so that the output data can be directly imported into PowerBI for visualization.
-  Read in parquet files and write into json-formatted files.

spark-submit gamestats_converter.py game_stats gamestats_output_json


event_visualization.py

-  Code to aggregate comment/post counts from Reddit and Twitter and combine that with the final event detection summarization for visualization on PowerBI. 
-  Read in the json files (gamestats_output_json) to construct a base timeline for each game.
-  Read in the parquet files (nhl_comments_final_v2) for Reddit comments consolidation.
-  Read in the parquet files (unified_tweets) for Tweets consolidation.
-  Read in the parquet files (new_tweet_with_prediction_v2) for merging event detection summary into the base timeline for each game.
-  Write into json-formatted files for visualization on PowerBI.

spark-submit event_visualization.py gamestats_output_json nhl_comments_final_v2 unified_tweets new_tweet_with_prediction_v2 visualizaiton_output


visualization_combiner.py

-  Code to combine the gamestats output file from gamestats_converter.py with the event detection output file from event_visualization.py

spark-submit visualization_combiner.py gamestats_output_json visualizaiton_output output_combined


## Tableau Visualizations

We included CMPT_732_Project.twbx for anyone to see how we created the visualizations. The .twbx file is the that Tableau desktop uses, and allows visualizations to be recreated without the root data source (it creates a local copy in the .twbx file). This .twbx file contains the vast majority of the visualizations we used in our presentation.



## PowerBI Visualizations

The following two PowerBIdesktop files contain a sample dataset and visualization of it for gamestats and event prediction respectively.
- nhl_visualization_gamestats.pbix
- nhl_visualization_eventdetection.pbix

The files can be opened with PowerBI Desktop application for immediate access to the interactive graphs.
