import tweepy
from collections import OrderedDict
from typing import List, OrderedDict, Optional, Union

from datetime import datetime, timedelta

from mindsdb.integrations.libs.api_handler import APIHandler
import datetime
from datetime import datetime
from pandas import DataFrame

class TwitterHandler(APIHandler):
    """A class for handling connections and interactions with the Twitter API.

    Attributes:
        bearer_token (str): The consumer key for the Twitter app.
        api (tweepy.API): The `tweepy.API` object for interacting with the Twitter API.

    """
    def __init__(self, connection_data: OrderedDict[str, str]):
        """
        Args:
            connection_data (OrderedDict): An ordered dictionary containing the 'bearer_token' for the Twitter app API V2.
        """
        self.bearer_token = connection_data['bearer_token']
        self.client = tweepy.Client(self.bearer_token)

        

        #self._register_table(table_name='tweets', table_columns=self._get_table_tweets_columns, ta)

    def connect(self):
        """Authenticate with the Twitter API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""
        self.api = tweepy.Client(self.bearer_token)

    def _get_table_tweets_columns(self) -> List[str]:
        return [
            'id', 
            'from'
            'to', 
            'conversation_id', 
            'text',
            'created_at', 
            'is_retweet',
            'is_reply',
            'is_quote',
            'is_verified',
            'is_nullcast',
            'hashtags',
            'cashtags',
            'links',
            'mentions',
            'media',
            'lang',
            'context',
            'entities', 
            'place', 
            'place_country',
            'place_geo_coordinates'
            'attachments', 
            'public_metrics', 
        ]

    def _query_tweets_table(self, 
        id: Optional[Union[int, str]] = None,
        from_user: Optional[Union[List, str]] = None
        
        ) -> DataFrame:
        
        # api stuff to get from tweets
        expansions = ['author_id', 'referenced_tweets.id', 'referenced_tweets.id.author_id', 'entities.mentions.username', 'attachments.poll_ids', 'attachments.media_keys', 'in_reply_to_user_id', 'geo.place_id', 'edit_history_tweet_ids']
        media_fields = ['media_key', 'media_type', 'url', 'duration_ms', 'height', 'public_metrics', 'width', 'alt_text']
        tweet_fields = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at',  'edit_history_tweet_ids', 'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang',   'possibly_sensitive',  'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        place_fields = []
        poll_fields = []

        user_fields = ['id', 'name']

        df = DataFrame(columns = self._get_table_tweets_columns)

        query_string = ''

        if (id):
            tweet = self.client.get_tweet(id = id, expansions = expansions, media_fields=  media_fields, place_fields = place_fields, tweet_fields = tweet_fields, user_fields = user_fields, poll_fields = poll_fields)
            row = self._get_row_from_tweet(tweet)
            df = df.append(row, ignore_index = True)
            return df

        if(from_user): 
            if type(from_user) == str:
                query_string = 'from: {from_user} '.format(from_user=from_user) # note what if its a list
            else:
                query_string = ' OR '.join([ 'from:{user}'.format(user = user) for user in from_user])

        if 


    def _get_table_tweets(self, screen_name: str, start_date: str = None, end_date: str = None, exclude: List[str] = ['retweets', 'replies'], limit: int=5) -> List[tweepy.models.Status]:
        """Retrieve a list of tweets from the specified user within the specified date range.

        Args:
            screen_name (str): The screen name of the user whose tweets you want to retrieve.
            start_date (str): The start date for the tweets in the format 'YYYY-MM-DD'.
            end_date (str): The end date for the tweets in the format 'YYYY-MM-DD'.
            limit (int, optional): The maximum number of tweets to retrieve. Default is 200.

        Returns:
            List[tweepy.models.Status]: A list of `tweepy.models.Status` objects representing the tweets.

        """
        # Get the tweets
        tweets = []
        expansions = ['author_id', 'referenced_tweets.id', 'referenced_tweets.id.author_id', 'entities.mentions.username', 'attachments.poll_ids', 'attachments.media_keys', 'in_reply_to_user_id', 'geo.place_id', 'edit_history_tweet_ids']
        tweet_fields = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at',  'edit_history_tweet_ids', 'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang',   'possibly_sensitive',  'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        user_fields = ['id', 'name']
        user = self.client.get_user(username=screen_name)
        #start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
        #end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return self.client.get_users_tweets(id=user.data.id, start_time=start_date, end_time=end_date, exclude = exclude, user_fields=user_fields, expansions = expansions, tweet_fields = tweet_fields, max_results=limit)
        for tweet in tweepy.Cursor(self.api.user_timeline, screen_name=screen_name, tweet_mode='extended').items():
            if tweet.created_at.date() >= start_date and tweet.created_at.date() <= end_date:
                tweets.append(tweet)
            if len(tweets) == limit:
                break
        return tweets


    

    def _search_tweets(self,
        query: str, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        limit: Optional[int] = None, 
        sort_order: Optional[str] = None, 
        next_token: Optional[str] = None, 
        since_id: Optional[Union[int, str]] = None, 
        until_id: Optional[Union[int, str]] = None,
        place_fields: Optional[Union[List[str], str]] = None, 
        poll_fields: Optional[Union[List[str], str]] = None,
        expansions: Optional[Union[List[str], str]] = None,
        tweet_fields: Optional[Union[List[str], str]] = None,
        user_fields: Optional[Union[List[str], str]] = None,
        media_fields: Optional[Union[List[str], str]] = None) -> None:
        
        """
        Search for tweets matching the given query.

        Parameters:
        - query (str): One query for matching Tweets. Up to 1024 characters.
        - start_time (datetime.datetime | str | None): YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). The oldest UTC timestamp 
        from which the Tweets will be provided. Timestamp is in second granularity and is inclusive (for example, 
        12:00:01 includes the first second of the minute). By default, a request will return Tweets 
        from up to 30 days ago if you do not include this parameter.
        - end_time (datetime.datetime | str | None): YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). Used with start_time. 
        The newest, most recent UTC timestamp to which the Tweets will be provided. Timestamp is in second granularity 
        and is exclusive (for example, 12:00:01 excludes the first second of the minute). If used without start_time, 
        Tweets from 30 days before end_time will be returned by default. If not specified, end_time will default to 
        [now - 30 seconds].
        - max_results (int | None): The maximum number of search results to be returned by a request. A number between 10 
        and the system limit (currently 500). By default, a request response will return 10 results.
        - sort_order (str | None): This parameter is used to specify the order in which you want the Tweets returned. 
        By default, a request will return the most recent Tweets first (sorted by recency).
        - next_token (str | None): This parameter is used to get the next ‘page’ of results. The value used with the 
        parameter is pulled directly from the response provided by the API, and should not be modified. You can learn 
        more by visiting our page on pagination.
        - since_id (int | str | None): Returns results with a Tweet ID greater than (for example, more recent than) 
        the specified ID. The ID specified is exclusive and responses will not include it. If included with the same 
        request as a start_time parameter, only since_id will be used.
        - until_id (int | str | None): Returns results with a Tweet ID greater than (for example, more recent than) 
        the specified ID. The ID specified is exclusive and responses will not include it. If included with the same 
        request as a start_time parameter, only since_id will be used.
        - place_fields (list[str] | str | None): place_fields
        - poll_fields (list[str] | str | None): poll_fields
        - media_fields (list[str] | str | None): media_fields
        """

        if limit <= 500:
            max_results = limit
        else:
            max_results = 500 # for now, this function must implement pagination and should be an iterator

        tweets = self.client.search_all_tweets(query=query, end_time=end_time, expansions=expansions, max_results=max_results, 
                media_fields=media_fields, next_token=next_token, place_fields=place_fields, poll_fields=poll_fields, 
                since_id=since_id, sort_order=sort_order, start_time=start_time, tweet_fields=tweet_fields, until_id=until_id, 
                user_fields=user_fields)

        return tweets

    def _is_within_last_7_days(date_string: str) -> bool:
        # Parse the date string
        date = datetime.fromisoformat(date_string)
        
        # Calculate the current date and time
        now = datetime.utcnow()
        
        # Calculate the difference between the current date and time and the given date
        diff = now - date
        
        # Return True if the difference is less than 6 days, and False otherwise
        return diff < timedelta(days=7)



# Replace these with your own API keys and secrets
connection_data = OrderedDict(
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAHO9kwEAAAAATIA4Dy9%2B7Vu6NFIxaHI3hI0Uctc%3Dm6gpXUUhl3KONDXtzfxW5a4J6OJWXfBgAoX278jnzzXguRS37t'
)

# Create a TwitterHandler object
twitter = TwitterHandler(connection_data)

# Connect to the Twitter API
twitter.connect()

# Example usage: get the last 200 tweets from the user 'twitter_username' between January 1st and January 31st
tweets = twitter._get_table_tweets('elonmusk', exclude=['retweets'])


#print (tweets)
#Print the tweets
for tweet in tweets[0]:
    print(tweet.referenced_tweets)
    print("\n-----")


# connection_args = OrderedDict(
#     bearer_token={
#         'type': ARG_TYPE.STR,
#         'description': 'The twitter API V2 bearer token.'
#     }
# )

# connection_args_example = OrderedDict(
#     bearer_token='fasdfasdfasdfas'
# )