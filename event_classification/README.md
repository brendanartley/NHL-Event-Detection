# Event Classification

Our classification method was focused on using lexicons to classify tweets. If the text contained words such as shoot, goal, score etc, we would classify this comment as goal-related. If the number of comments containing these keywords exceeded a certain threshold then we predicted that a goal had occured.

When we were optimizing this method, we explored using a simple word counter and a TF-IDF method to find words that would be important in our classification.

## event_classification.py

`spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4 event_classification/event_classification.py event_associated_data gamestats/livefeed/parquet out`

The script takes 3 arguments below.

1. input file - the output of `event_detection/event_detection.py`
2. game event livefeed file
3. output file

When optimizing the classification part of the implementation, we found our initial method to be quite slow in its execution time. 

The script utilizes UDF rather than joins. We found that the script runs roughly half the time when compared with the initial version of our code. This allows us to test more lexicons, window sizes, and overall classification methods.

# Notes

The script here is classifying events into whether or not we are seeing a goal based on the lexicons and evaluating how accurate our predictions are based on custom metrics.

- What other lexicons should we add?

- How should we summarize after the lexicon operation?
    - Sum of lexicon count? Average lexicon count?


- How should we evaluate the model performance?
    - We have implemented two UDFs that evaluate;
        1. percentage of predicted events that are within 60 seconds of a goal
        2. percentage of the actual goals that our predictions were able to capture.

I think we need to think about how we classify once the lexicon operation is applied (ie sum, average). We also need to test to see which window size is best.

(SOLVED) Note that the script works with the reddit_comments but for some reason outputs 0, 0 for both accuracies on the tweets. Need to figure out why.
