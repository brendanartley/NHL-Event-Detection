## simple_word_counter.py 
The code counts the number of words found in commments. It outputs a dataframe with the most common words found in comments after goals, and the most common words found in all other comments. 

- inputs
  - Cleaned reddit data after ETL
- outputs
  - Word counts of most common words after goals and in the other times respectively.

Side Note: This script takes in two inputs. We currently have them hard-coded in, but this script can be modified to do a word count of spark dataframe column of strings. Simply rename a column in the data frame to "body", and remove a few filtering methods that are specific to our use case, and you are good to go.

This script uses spark-nlp, and can be ran with the following command line.

`spark-submit --jars spark-nlp-assembly-3.3.4.jar simple_word_counter.py cleaned_reddit_parquet output_path`


## event_text_cleaning.py
The code cleans up texts before fedding them into `tf_idf.py`.

- inputs
  - the output of event_detection.py
- outputs
  - cleaned texts

`spark-submit --jars spark-nlp-assembly-3.3.4.jar lexicon_generation/event_text_cleaning.py event_detected_window_path output_path`


## tf_idf.py
The code calculates a TF-IDF score for each word in reddit comments or twitter texts.

- inputs
  - the output of event_text_cleaning.py
- output
  - TF-IDF scores

`spark-submit --jars spark-nlp-assembly-3.3.4.jar lexicon_generation/event_text_cleaning.py cleaned_window_path output_path`
