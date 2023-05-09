from TikTokApi import TikTokApi
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities.config import Config
import os

class TikTokHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('tiktok_handler', {})
        for k in ['client_id', 'client_secret', 'app_id']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'TIKTOK_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'TIKTOK_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.is_connected = False