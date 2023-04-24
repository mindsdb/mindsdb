import os
import datetime as datetime
import ast
from typing import List
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import openai

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

class SlackHandler(APIHandler):
    """
    
    A class for handling connections and interactions with Slack API.
    Attributes:
        bot_token(str): The bot token for the Slack app.
        api(slack_sdk.WebClient): The `slack_sdk.WebClient` object for interacting with the Slack API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.connection_args = {}
        handler_config = Config().get('slack_handler', {})
        for k in ['token']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'SLACK_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'SLACK_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
        self.api = None
        self.is_connected = False

        # channels = SlackChannelsTable(self)
        channels = "#test_channels"
        self._register_table('channels', channels)

    def create_connection(self):
        client = WebClient(token=self.connection_args['token'])
        return client
    
    def connect(self):
        """
        Authenticate with the Slack API using the token stored in the `token` attribute.
        """

        if self.is_connected is True:
            return self.api

        self.api = self.create_connection()
        return self.api

    def check_connection(self):
        response = StatusResponse(False)

        try:
            api = self.connect()

            # Call API method to check the connection
            api.auth_test()
            
            response.success = True
            print("Connected to Slack API.")
        except SlackApiError as e:
            response.error_message = f'Error connecting to Slack Api: {e.response["error"]}. Check token.'
            print(f"Error connecting to Slack API: {e}")
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_slack_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_slack_api(self, method_name: str = None, params: dict = None):
        api = self.connect()
        method = getattr(api, method_name)

        try:
            result = method(**params)

        except SlackApiError as e:
            error = f"Error calling method '{method_name}' with params '{params}': {e.response['error']}"
            log.logger.error(error)
            raise e

        if 'channels' in result:
            result['channels'] = self.convert_channel_data(result['channels'])

        return [result]

    def convert_channel_data(self, channels: List[dict]):
        """
        Convert the list of channel dictionaries to a format that can be easily used in the data pipeline.

        Args:
            channels: A list of channel dictionaries.

        Returns:
            A list of channel dictionaries with modified keys and values.
        """

        new_channels = []
        for channel in channels:
            new_channel = {
                'id': channel['id'],
                'name': channel['name'],
                'created': datetime.fromtimestamp(float(channel['created']))
            }
            new_channels.append(new_channel)
        return new_channels

