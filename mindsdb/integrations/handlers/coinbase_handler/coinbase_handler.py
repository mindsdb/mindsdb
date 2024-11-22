import time
import hmac
import base64
import hashlib
import datetime
from typing import Dict

import pandas as pd
import requests

from mindsdb.integrations.handlers.coinbase_handler.coinbase_tables import CoinBaseAggregatedTradesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

from mindsdb_sql_parser import parse_sql

_BASE_COINBASE_US_URL = 'https://api.exchange.coinbase.com'


class CoinBaseHandler(APIHandler):
    """A class for handling connections and interactions with the CoinBase API.

    Attributes:
        api_key (str): API key
        api_secret (str): API secret
        is_connected (bool): Whether or not the API client is connected to CoinBase.

    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.api_key = None
        self.api_secret = None
        self.api_passphrase = None

        args = kwargs.get('connection_data', {})
        if 'api_key' in args:
            self.api_key = args['api_key']
        if 'api_secret' in args:
            self.api_secret = args['api_secret']
        if 'api_passphrase' in args:
            self.api_passphrase = args['api_passphrase']
        self.client = None
        self.is_connected = False

        coinbase_candle_data = CoinBaseAggregatedTradesTable(self)
        self._register_table('coinbase_candle_data', coinbase_candle_data)

    def connect(self):
        """Creates a new CoinBase API client if needed and sets it as the client to use for requests.

        Returns newly created CoinBase API client, or current client if already set.
        """
        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Checks connection to CoinBase API by sending a ping request.

        Returns StatusResponse indicating whether or not the handler is connected.
        """
        response = StatusResponse(True)
        self.is_connected = response.success
        return response

    # symbol: BTC-USD
    # granularity 60, 300, 900, 3600, 21600, 86400
    def get_coinbase_candle(self, symbol: str, granularity: int) -> pd.DataFrame:
        jdocs = []
        current_time = datetime.datetime.now()
        start_time = current_time - datetime.timedelta(seconds=granularity)
        start_time_iso = start_time.isoformat().split(".")[0] + "-04:00"
        path = "/products/" + symbol + "/candles?granularity=" + str(granularity) + "&start=" + start_time_iso
        headers = self.generate_api_headers("GET", path)
        url = _BASE_COINBASE_US_URL + path
        response = requests.get(url, headers=headers)
        candles = response.json()
        for candle in candles:
            dt = datetime.datetime.fromtimestamp(candle[0], None).isoformat()
            low, high, open, close, volume = candle[1:]
            jdoc = {"symbol": symbol, "low": low, "high": high, "open": open, "close": close, "volume": volume, "timestamp": candle[0], "timestamp_iso": dt}
            jdocs.append(jdoc)
        return pd.DataFrame(jdocs)

    def _get_candle(self, params: Dict = None) -> pd.DataFrame:
        """Gets aggregate trade data for a symbol based on given parameters

        Returns results as a pandas DataFrame.

        Args:
            params (Dict): Trade data params (symbol, interval)
        """
        if 'symbol' not in params:
            raise ValueError('Missing "symbol" param to fetch trade data for.')
        if 'interval' not in params:
            raise ValueError('Missing "interval" param (60, 300, 900, 3600, 21600, 86400).')

        candle = self.get_coinbase_candle(params['symbol'], int(params['interval']))
        return candle

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def generate_api_headers(self, method: str, path: str) -> dict:
        timestamp = str(int(time.time()))
        message = timestamp + method + path
        signature = base64.b64encode(hmac.new(base64.b64decode(self.api_secret), str.encode(message), hashlib.sha256).digest())
        headers = {
            "Content-Type": "application/json",
            "CB-ACCESS-SIGN": signature,
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-VERSION": "2015-04-08",
            "CB-ACCESS-PASSPHRASE": self.api_passphrase
        }
        return headers

    def call_coinbase_api(self, method_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the CoinBase API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call
            params (Dict): Params to pass to the API call
        """
        if method_name == 'get_candle':
            return self._get_candle(params)
        raise NotImplementedError('Method name {} not supported by CoinBase API Handler'.format(method_name))
