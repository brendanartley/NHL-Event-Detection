1. Twint quirks
    1. it cannot retreive historcal tweets beyond 10 days
        But there's a fix for that: uncomment line 92 of `url.py` in twint/
        There is a pull request on the github repo for this, but not merged yet.
        You might want to make this change as well. Works for MacOS.
        [original issue link]("https://github.com/twintproject/twint/issues/1266")

        Now it works happily again.
        ```
        ~ twint -s "nhl" --since "2021-10-12" --until "2021-10-13" --count
        [+] Finished: Successfully collected 8717 Tweets.
        ```
    2. the search term character limit is 500
    3. it assumes the date you give it is UTC time, and would convert it to local time for you. the granularity of time is day. PST and PDT wouldn't be considered.

2. The keyword generation method is still crude. Maybe I should do this:
   1. crawl all tweets associated with a team name
   2. find the keywords in them somehow
       - could use some fancy ML methods
   3. use those keywords to crawl game-related tweets
   4. crawl tweets in game day, filter the tweets using:
      1. "nhl", "hockey"
      2. name of teams: use the official abbreviation, short name and the fullname
      3. ~~player names~~ this doesn't work
      4. hockey jargons
      5. actual timeslot

3. Filter down the tweets
    1. time

4. `Minnesota Wild` has wild in its name. Not so good.
    This was found out by looking at the number of tweets around a scoring event. The count is super high for this team.

5. [This](https://stackoverflow.com/questions/57807336/pandas-udf-operating-on-two-arraytypestringtype-fields) is better than actually sliding the window.