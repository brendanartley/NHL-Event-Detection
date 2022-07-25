## NHL Event Detection

[@brendanartley](https://github.com/brendanartley), [@grovershreyf9t](https://github.com/grovershreyf9t), [@bofei-jing](https://github.com/bofei-jing), [@AndrewHan](https://github.com/beinfluential88)

In this project, we implement a POC that uses social media activity to predict goals during NHL (National Hockey League) games. This project shows how social media can be used to detect significant events. The proposed architecture can be modified to detect natural disasters, traffic incidents, large gatherings, and much more.

The main components of this project were to:
- identify relevant tweets and comments from social media
- process incoming tweets with little delay
- develop a model to detect events
- develop a model to classify events
- evaluate results
- improve on the last iteration

My groupmates and I put together [this video](https://www.youtube.com/watch?v=AGMcEMRGVVA) explaining the project in more detail.

### Data

The final model included three unique data sources. These were social media data from Reddit and Twitter, and NHL event data from the NHL API. These data sources required mining, cleaning, and engineering to get them into a usable format.

- Reddit Corpus Dataset (~500GB)
- Scraped Twitter Data (~50GB)
- NHL API (~2GB)

### Reddit

Reddit has a subreddit for every NHL team and one general hockey subreddit. Each subreddit has what is called a game thread, where users can comment live during a game. These were introduced to Reddit in the last few years and therefore we were only able to find data from 2020 onwards. You can see from the following plot how many comments are being made in these game threads by year. Since we were working with Reddit data up June 2021, we included the projected number of comments that year. 

![Reddit Activity in Game Threads](./imgs/comments_over_time.png)

Moreover, we can see the comment activity in each subreddit below.

![Activity by Subreddit](./imgs/sub_comment_count1.png)

The highest activity was from the game thread in the "hockey" subreddit. This makes sense as there are threads for every single game here regardless of the team. In the individual subreddits, the game threads are only for their team. When we remove the hockey subreddit from the plot we can compare each team's subreddit activity closely. Some teams did not adopt game threads until mid-way through 2020 and others simply have low activity levels. We can see that the Vancouver Canucks have a very active fanbase on Reddit, whereas the Anaheim Ducks do not.

![Activity by Team's Subreddit](./imgs/sub_comment_count2.png)
 
### Twitter

[Twitter](https://developer.twitter.com/en/docs/developer-portal/overview) was slightly different than the Reddit data. We had to acquire data based on hashtags and mentions in the actual tweets. This meant that the data acquired would not be as comprehensive and would be biased towards the filtering process. That being said, we were still able to acquire a large number of tweets for each team. Based on our results the most active fans on Twitter were supporters of the Minnesota Wild, and the least active fanbase was the Columbus Blue Jackets. 

![Activity of Fans on Twitter](./imgs/tweets.png)

### NHL API

The [NHL API](https://gitlab.com/dword4/nhlapi) was the cleanest of the three data sources and provided much more information than we expected. Given the complexity of this data, many really interesting projects can be created from this source. In our case, we only needed the start time, end time, and goal times for each match. We were able to collect this data using various endpoints and asynchronous scripts. Thanks to [@Drew Hynes](https://dword4.github.io/) for documenting the API.

### Event Detection

For detecting events, we used a hopping window method. For each window, an event was detected if the post rate exceeded 1 post per second, and if there was more comment activity in the second part of the window than in the first. After testing window sizes of 6, 10, 20, 30, and 60 seconds, we found that a 60-second window worked best.

### Event Classification

The event classification step used lexicon-based methods and clustering to classify goal events from non-goal events. Some of the lexicons used were "goal", "score", "lead", "net", "assist", "play", "pass". If a comment contained one or more of these lexicons, then a comment was labeled as related to a goal. If the average number of comments in a window exceeded a certain threshold then we classified the mid-point of this window as the time of the goal.

Initially, we had many windows that were predicting the same goal. This was problematic and we decided to implement a simple clustering algorithm to solve this problem. By traversing a sorted list of predicted goals during a match, predictions were grouped together if they fell within 30 seconds of their neighbor. Using the earliest and latest timestamp in the cluster, we took the average of these two values and used this as the predicted time of the goal. This clustering step was paramount to increasing the precision and decreasing the inference time of the model.

![Clustering Algorithm](./imgs/clustering_algorithm.png)

### Results

For the Reddit comments, we achieve a precision accuracy of 72% and a recall accuracy of 50%. For the Twitter comments, we achieved a precision accuracy of 80% and a recall accuracy of 36%. A prediction was classified as correct if it fell within 30 seconds of the true goal time. We can see the results of the model in the plots below.

![Precision Accuracy](./imgs/precision_accuracy.png)
![Recall Accuracy](./imgs/recall_accuracy.png)
