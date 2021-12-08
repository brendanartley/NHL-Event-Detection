# Notes

The script here is classifying wether we are seeing a goal or not based on the lexicons.

- What other lexicons should we add?

- How should we classify after lexicon operation?
    - Sum of lexicon count? Average lexicon count?

- How should we evaluate model performance?
    - I implemented two udfs that evaluate
        - percentage of predicted events that are within 60 seconds of a goal
        - percentage of the goals that our predictions captured

I Think we need to think about how we classify once the lexicon operation is applied. We also need to test to see which window size is best.

Note that the script works with the reddit_comments but for some reason outputs 0, 0 for both accuracies on the tweets. Need to figure out why.

## Running event_classification.py

spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4 event_classification.py event_associated_tweets game_stats out reddit

The script takes 3 arguments

1. input comments file
2. game_stats file
2. output file
3. comment source (reddit or twitter) 

## ec_2.py

2nd option for the classification part. This script uses UDF's rather that joins. Seems to be faster for the reddit data. More rigorous testing needed here.