import pandas as pd
from typing import Dict

from binance.spot import Spot

from mindsdb.integrations.handlers.binance_handler.binance_tables import BinanceAggregatedTradesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

_BASE_BINANCE_US_URL = 'https://api.binance.us'

logger = log.getLogger(__name__)


class BinanceHandler(APIHandler):
    """A class for handling connections and interactions with the Binance API.

    Attributes:
        client (binance.spot.Spot): The `binance.spot.Spot` object for interacting with the Binance API.
        api_key (str): API key, only required for user data endpoints.
        api_secret (str): API secret, only required for user data endpoints.
        is_connected (bool): Whether or not the API client is connected to Binance.

    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.api_key = None
        self.api_secret = None

        args = kwargs.get('connection_data', {})
        if 'api_key' in args:
            self.api_key = args['api_key']
        if 'api_secret' in args:
            self.api_secret = args['api_secret']

        self.client = None
        self.is_connected = False

        aggregated_trade_data = BinanceAggregatedTradesTable(self)
        self._register_table('aggregated_trade_data', aggregated_trade_data)

    def connect(self) -> Spot:
        """Creates a new Binance Spot API client if needed and sets it as the client to use for requests.

        Returns newly created Binance Spot API client, or current client if already set.
        """
        if self.is_connected is True and self.client:
            return self.client

        if self.api_key and self.api_secret:
            self.client = Spot(key=self.api_key, secret=self.api_secret, base_url=_BASE_BINANCE_US_URL)
        else:
            self.client = Spot(base_url=_BASE_BINANCE_US_URL)

        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Checks connection to Binance API by sending a ping request.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(False)

        try:
            client = self.connect()
            client.ping()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Binance API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def _get_klines(self, params: Dict = None) -> pd.DataFrame:
        """Gets aggregate trade data for a symbol based on given parameters

        Returns results as a pandas DataFrame.

        Args:
            params (Dict): Trade data params (symbol, interval, limit, start_time, end_time)
        """
        if 'symbol' not in params:
            raise ValueError('Missing "symbol" param to fetch trade data for.')
        if 'interval' not in params:
            raise ValueError('Missing "interval" param (1m, 1d, etc).')

        client = self.connect()
        symbol = params['symbol']
        interval = params['interval']
        limit = params['limit'] if 'limit' in params else BinanceAggregatedTradesTable.DEFAULT_AGGREGATE_TRADE_LIMIT
        start_time = params['start_time'] if 'start_time' in params else None
        end_time = params['end_time'] if 'end_time' in params else None
        raw_klines = client.klines(
            symbol,
            interval,
            limit=limit,
            startTime=start_time,
            endTime=end_time)

        open_time_i = 0
        close_time_i = 6
        for i in range(len(raw_klines)):
            # To train we need timestamps to be in seconds since Unix epoch and Binance returns it in ms.
            raw_klines[i][open_time_i] = int(raw_klines[i][open_time_i] / 1000)
            raw_klines[i][close_time_i] = int(raw_klines[i][close_time_i] / 1000)

        df = pd.DataFrame(raw_klines)
        df.insert(0, 'symbol', [symbol] * len(raw_klines), True)
        # Remove last unnecessary column (unused API field)
        if len(raw_klines) > 0:
            num_cols = len(df.columns)
            df = df.drop(df.columns[[num_cols - 1]], axis=1)
        return df

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def call_binance_api(self, method_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the Binance API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call (e.g. klines)
            params (Dict): Params to pass to the API call
        """
        if method_name == 'klines':
            return self._get_klines(params)
        raise NotImplementedError('Method name {} not supported by Binance API Handler'.format(method_name))
