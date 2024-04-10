import requests
import pandas as pd
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from typing import Optional
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse, HandlerResponse as Response, RESPONSE_TYPE
from .cryptopanic_tables import NewsTable
from utils.cryptopanic_api import call_cryptopanic_api

logger = log.getLogger(__name__)

class CryptoPanicHandler(APIHandler):
    """
    A class for handling connections and interactions with the Hacker News API.
    """

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        
        self.api_token = connection_data["api_token"]

        
        news = NewsTable(self)
        self._register_table('news', news)

    def connect(self):
        return

    def check_connection(self) -> StatusResponse:
        try:
            response = call_cryptopanic_api(self.api_token)
            response.raise_for_status()
            return StatusResponse(True)
        except Exception as e:
            logger.error(f'Error checking connection: {e}')
            return StatusResponse(False, str(e))

    def native_query(self, query_string: str = None):
        # method_name, params = self.parse_native_query(query_string)

        df = self.query(query_string)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
