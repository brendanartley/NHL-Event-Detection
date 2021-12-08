## Visualizations ETL

Given that we are working with a large amount of data, we had to do even more filtering and aggregating to make the visualizations possible. It was doable using all the data, but doing so in an interactive way was not doable with large amounts of data. 

Once we simplified the data into something more manageable we were able to create some pretty good visualizations.

## Scripts

wordcloud_prep.py

-  code to aggregate the data and create a frequency count of words found in comments after goals and all other comments. Using the outputted CSV file of 200 rows. We created a wordplot in Tableau.

spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4  wordcloud_prep.py