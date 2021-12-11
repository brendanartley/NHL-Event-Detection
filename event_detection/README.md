## Event Detection

The event detection method we implemented was based on a sliding window technique. For a window to be flagged as containing an event, there must be at least one comment per second in the entire window, and there has to be more activity in the second half of the window than the first. We tested window sizes of 6, 10, 20, 30, and 60 seconds. 

When we were optimizing this method we explored using a simple word counter and a TF-IDF method to find words that would be important in our classification.

simple_word_counter.py - outputs a dataframe with the most common words found in comments after goals, and the most common words found in all other comments. 

### windowing.py
- inputs    
  1. path to unified reddit/tweet dataset
  2. window size (60)
  3. sliding step size (1)
- output
  - windowed dataset

`spark-submit event_detection/windowing.py input_unified_parquet_file_path window_size sliding_step_size output_path`


### event_detection.py
- inputs
  - path to windowed dataset
- output
  - windows with event in them

`spark-submit event_detection/event_detection.py input_windowed_parquet_file_path output_path`
