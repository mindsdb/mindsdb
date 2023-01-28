import tweepy
import ast
import numbers
from collections import OrderedDict
from typing import List, OrderedDict, Optional, Union
from pprint import pprint
from mindsdb.utilities import log

from mindsdb_sql.parser.ast.base import ASTNode

from datetime import datetime, timedelta

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, Response, RESPONSE_TYPE
import datetime
from datetime import datetime
import pandas as pd

##api_key secret = Pt9Xmm0CgxRbUCVqac3SZMYXvsIxArF1q5NqzvC
##api key = JQldSGOLxozplwCjqKongdYtr
##access token = 1268636021811535874-7Qsrt99HspRUolTrOrsNyFg2jBiI48
##access token secret = 823mvY0fyv4eS3dby55BcHAOU3M3hswQMRF5cTXMJfh0A

class TweetsTable(APITable):

    
    def select(self, query: ASTNode) -> Response:

        ## parse the query and 

        args_from_query= {}


        query_template = '''
        search_recent_tweets(
            query="{{query_str}}",
            max_results={{limit}}
            
        )
        '''.format(**args_from_query)

        # read: https://docs.tweepy.org/en/stable/client.html#search-tweets
        # for query attribute, read: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query

        
        results = self.handler.native_query( query_template )

        return results


    def get_columns():
        return [
            'id', 
            'author_id'
            'username',
            'name', 
            'conversation_id', 
            'text',
            'created_at', 
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
            'public_metrics' 
        ]
        

    def insert(handler, dataframe):
        pass

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
        super().__init__(self)
        self.bearer_token = connection_data['bearer_token']
        
        tweets =  TweetsTable(self)
        self._register_table('tweets', tweets)

        

        #self._register_table(table_name='tweets', table_columns=self._get_table_tweets_columns, ta)

    def connect(self):
        """Authenticate with the Twitter API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""
        
        try:
            self.api = tweepy.Client('AAAAAAAAAAAAAAAAAAAAAHrmlQEAAAAAe5TehqJ0k3%2BoUtfO7QGYZYIAjMc%3Dy30HdhMQBEoxersUHNsegrtBrGuriYbkS3YzF0L6V7cCwxLTeA')
        except Exception as e:
            log.logger.error(f'Error running while getting table {e} on ')
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        self.is_connected = True

        return Response(RESPONSE_TYPE.OK)
    
    
    def native_query(self, query_string: str):

        if self.is_connected == False:
            self.connect()

        params = {}
        node = ast.parse(query_string)
        call = node.body[0].value
        method_name = call.func.id
        method = getattr(self.api, method_name)


        # the twitter api is a bit confusing, so lets help people at least for search tweets
        # Note: search_all_tweets is only available for researchers
        if method_name in ['search_recent_tweets', 'search_all_tweets']:
            if 'expansions' not in params:
                params['expansions']='author_id'
            if 'tweet_fields' not in params:
                params['tweet_fields']=['created_at']
            if 'user_fields' not in params:
                params['user_fields'] = ["name", "username"]

        for param in call.keywords:
            params[param.arg] = ast.literal_eval(param.value)
        
        if 'max_results' in params:
            if params['max_results'] > 100:
                # next implement pagination
                # but for now keep it at 100
                params['max_results'] = 100
                
        try:
            batches = [method(**params)]
        except Exception as e:
            log.logger.error(f'Error running while getting table {e} on ')
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        
        # if response is nothing, return empty ok response
        if len(batches[0].data) == 0:
            return Response(RESPONSE_TYPE.OK)

        # get the columns for the response
        main_df_columns = batches[0].data[0].data.keys() 

        # get uers if they exist
        #  tweets.includes['users']

        data = pd.DataFrame(columns=main_df_columns)
        if 'users' in batches[0].includes:
            users_table_columns = batches[0].includes['users'][0].data.keys()
            users = pd.DataFrame(columns=users_table_columns)

        for batch in batches:
            # create the main table to return
            for row in batch.data:
                row_dict = {col: row[col] if isinstance(row[col], numbers.Number) else str(row[col]) for col in main_df_columns}
                row_df = pd.DataFrame(row_dict, index=[0])
                data = pd.concat([data, row_df])

            # create the users dataframe if users data was requested
            if 'users' in batch.includes:
                for row in batch.includes['users']:
                    row_df = pd.DataFrame(row.data, index=[0])
                    users = pd.concat([users, row_df])
                
                #lastly we should merge data and users
                # Andrey: ***** Note, this left join is not working, but it should, I just dont know pandas well
                # if you print(users) you will see that there are users for every row, 
                data = pd.merge(left=data, right=users, left_on='author_id', right_on='id', how='left', suffixes=("", "_user"))
                
        
            
        return Response(
                RESPONSE_TYPE.TABLE,
                data_frame= data
        )
        

    
    

    



# Replace these with your own API keys and secrets
connection_data = OrderedDict(
    bearer_token = 'blah'
)

# Create a TwitterHandler object
twitter = TwitterHandler(connection_data)

# Connect to the Twitter API
twitter.connect()

tweets = twitter.native_query('''
search_recent_tweets(
    query="elonmusk -is:retweet",
    max_results=10
    
)
''')

# show the results
pprint(tweets.data_frame)

