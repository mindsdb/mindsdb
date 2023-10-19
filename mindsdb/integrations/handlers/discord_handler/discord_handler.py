import os
import asyncio
import aiohttp

from discord import Webhook

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response)
from mindsdb_sql import parse_sql


class DiscordHandler(APIHandler):
    """A class for handling connections and interactions with the Discord API.

    Attributes:
    webhook_url(str): URL for your Discord server's webhook
    """

    def __init__(self, name = None, **kwargs):
        super().__init__(name)
        self.loop = asyncio.get_event_loop()
        args = kwargs.get('connection_data', {})
        if 'webhook_url' not in args:
            raise ValueError('Must include webhook url')
        self.connection_args = {}
        handler_config = Config().get('discord_handler', {})
        for k in ['webhook_url']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'DISCORD_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'DISCORD_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.webhook = None
        self.is_connected = False

    async def __create_connection(self):
        async with aiohttp.ClientSession() as session:
            self.webhook = Webhook.from_url(self.connection_args['webhook_url'], session=session)
            return self.webhook
        
    def connect(self):
        return self.loop.run_until_complete(self.__create_connection())

    def check_connection(self) -> StatusResponse:
    
        response = StatusResponse(False)
        try:
            self.connect()
            
            response.success = True
        except Exception as e:
            log.logger.error(f"Error connecting to Discord Webhook: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query, dialect = 'mindsdb')
        return self.query(ast)

    async def __send_message(self, message, username):
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(self.connection_args["webhook_url"], session=session)
            await webhook.send(content = message, username = username)

    def message(self,message,username):
        return self.loop.run_until_complete(self.__send_message(message,username))

    async def __send_announcement(self, message, username):
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(self.connection_args["webhook_url"], session=session)
            message='@everyone ' + message
            await webhook.send(content = message, username = username)

    def announce(self,message,username):
        return self.loop.run_until_complete(self.__send_announcement(message, username))
