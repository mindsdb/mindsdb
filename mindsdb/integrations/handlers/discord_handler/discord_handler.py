import requests
import asyncio
import pandas as pd

from mindsdb.integrations.handlers.discord_handler.discord_tables import DiscordTable

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

discord_bot = None

class DiscordHandler(APIHandler):
    """
    The Discord handler implementation.
    """

    name = 'discord'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.is_authorized = False

        messages = DiscordTable(self)
        self._register_table('messages', messages)

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """

        if self.is_authorized:
            return StatusResponse(True)

        url = f'https://discord.com/api/v10/applications/@me'
        result = requests.get(
            url, 
            headers={
                'Authorization': f'Bot {self.connection_data["token"]}',
                'Content-Type': 'application/json',
            }, 
        )

        if result.status_code != 200:
            raise ValueError(f'Error connecting to Discord: {result.json()}')

        self.is_authorized = True

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Discord!')
            response.error_message = str(e)

        self.is_authorized = response.success

        return response

    def native_query(self, query: str = None) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        method_name, params = FuncParser().from_string(query)

        df = self.call_discord_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
    
    def utc_to_snowflake(self, utc_date: str) -> int:
        """
        Convert a UTC date to a Snowflake date.
        Args:
            utc_date (str): the UTC date
        Returns:
            int
        """
        # https://discord.com/developers/docs/reference#snowflakes
        return str(int(parse_utc_date(utc_date).timestamp() * 1000 - 1420070400000) << 22)

    def call_discord_api(self, method_name: str, params: dict = None, filters: list = None):
        """
        Call a Discord API method.
        Args:
            method_name (str): the method name
            params (dict): the method parameters
        Returns:
            pd.DataFrame
        """
        
        if method_name == 'get_messages':
            param_strings = { 'limit': params['limit'] }
            if 'after' in params:
                param_strings['after'] = self.utc_to_snowflake(params['after'])
            if 'before' in params:
                param_strings['before'] = self.utc_to_snowflake(params['before'])

            url = f'https://discord.com/api/v10/channels/{params["channel_id"]}/messages'
            result = requests.get(
                url, 
                headers={
                    'Authorization': f'Bot {self.connection_data["token"]}',
                    'Content-Type': 'application/json',
                },
                params=param_strings,
            )

            if result.status_code != 200:
                raise ValueError(f'Error calling Discord API: {result.json()}')

            json = result.json()
            for filter in filters:
                json = list(filter(result.json()))

            for msg in json:
                msg['author_id'] = msg['author']['id']
                msg['author_username'] = msg['author']['username']
                msg['author_global_name'] = msg['author']['global_name']

            df = pd.DataFrame.from_records(json)
            return df
        elif method_name == 'send_message':
            url = f'https://discord.com/api/v10/channels/{params["channel_id"]}/messages'
            result = requests.post(
                url,
                headers={
                    'Authorization': f'Bot {self.connection_data["token"]}',
                    'Content-Type': 'application/json',
                }, 
                json={
                    'content': params['message'],
                },
            )
 
            if result.status_code != 200:
                raise ValueError(f'Error calling Discord API: {result.json()}')

            df = pd.DataFrame.from_records([result.json()])
            return df
        else:
            raise ValueError(f"Unsupported method: {method_name}")