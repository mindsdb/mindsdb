from typing import Optional
from collections import OrderedDict

import pandas as pd
import duckdb
import io
import requests
import json

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

log = get_log()


class TwitterHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Twitter api 
    """

    name = 'twitter'
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'twitter'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def userid_lookup(self):

        userid_request_url = 'https://api.twitter.com/2/users/by/username/' + self.connection_data['twitter_user_name']

        headers = {
                    "Authorization": f"Bearer {self.connection_data['twitter_api_token']}",      
                    }
        response = requests.request("GET",url=userid_request_url,headers=headers)
        records = json.loads(response.text)
        user_id = records['data']['id']
        
        return user_id


    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        user_id = self.userid_lookup()
        tweets_url = 'https://api.twitter.com/2/users/' + str(user_id) + '/tweets'

        params = {
            ('max_results','100'),
            ('tweet.fields', 'author_id,created_at,text,public_metrics'),
            ('start_time', self.connection_data['tweets_start_time']),
            ('end_time', self.connection_data['tweets_end_time'])
        }
        
        headers = {
            "Authorization": f"Bearer {self.connection_data['twitter_api_token']}",
        }
        response = requests.request("GET",url=tweets_url,headers=headers,params=params)
        if response.status_code == 200:
            self.connection = response
        
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to twitter api, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        result = json.loads(connection.text)
        df = pd.json_normalize(result['data'])
        
        locals()[self.connection_data['twitter_endpoint_name']] = df

        try:            
            result = duckdb.query(query)
            if result:
                response = Response(RESPONSE_TYPE.TABLE,data_frame=result.df())                   
            else:
                response = Response(RESPONSE_TYPE.OK)

        except Exception as e:
            log.error(f'Error running query  {query} on twitter handler')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                [self.connection_data['twitter_endpoint_name']],
                columns=['table_name']
            )
        )

        return response

    def get_columns(self) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """
        query = 'SELECT * FROM tweets LIMIT 10'
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': result.data_frame.dtypes
                }
            )
        )

        return response


connection_args = OrderedDict(
    twitter_user_name={
        'type': ARG_TYPE.STR,
        'description': 'Twitter user name for extracting the tweets'
    },
    twitter_api_token={
        'type': ARG_TYPE.STR,
        'description': 'API Token for accessing the twitter data'
    },
    tweets_start_time={
        'type': ARG_TYPE.STR,
        'description': 'start time field for querying tweets'
    },
    tweets_end_time={
        'type': ARG_TYPE.STR,
        'description': 'end time field for querying tweets'
    },
    twitter_endpoint_name={
        'type': ARG_TYPE.STR,
        'description': 'twitter api end point name for requesting'
    }
  

)

connection_args_example = OrderedDict(
    twitter_user_name='elonmusk',
    twitter_api_token ='AAAAAAAAAAAAAAAAAAAAAKkrbgEAAAAARPWLaxlFo0vUOGavorPuTCzk1lM%3D2GJWOiEM15n5csdO42sm244kzyw9I0jIXEYxt6HmR7H3ZAcGXA',
    tweets_start_time='2023-01-01T10:00:50Z',
    tweets_end_time='2023-02-01T10:00:50Z',
    twitter_endpoint_name='tweets',
)
