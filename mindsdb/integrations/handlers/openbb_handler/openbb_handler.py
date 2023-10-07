import pandas as pd
from typing import Dict

from openbb import obb

from mindsdb.integrations.handlers.openbb_handler.openbb_tables import OpenBBtable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

class OpenBBHandler(APIHandler):
    """A class for handling connections and interactions with the OpenBB Platform.

    Attributes:
        PAT (str): OpenBB's personal access token. Sign up here: https://my.openbb.co
        is_connected (bool): Whether or not the user is connected to their OpenBB account.

    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.PAT = None

        args = kwargs.get('connection_data', {})
        if 'PAT' in args:
            self.PAT = args['PAT']

        self.is_connected = False

        stocks_load = OpenBBtable(self)
        self._register_table('stocks_load', stocks_load)

    def connect(self) -> bool:
        """Connects with OpenBB account through personal access token (PAT).

        Returns none.
        """
        self.is_connected = False
        obb.account.login(pat=self.PAT)
        
        # Check if PAT utilized is valid
        if obb.user.profile.active:
            self.is_connected = True
            return True
        
        return False

    def check_connection(self) -> StatusResponse:
        """Checks connection to OpenBB accounting by checking the validity of the PAT.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(False)

        try:
            if self.connect():
                response.success = True

        except Exception as e:
            log.logger.error(f'Error connecting to OpenBB Platform: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def _get_stocks_load(self, params: Dict = None) -> pd.DataFrame:
        """Gets aggregate trade data for a symbol based on given parameters

        Returns results as a pandas DataFrame.

        Args:
            params (Dict): Trade data params (symbol, interval, limit, start_time, end_time)
        """
        self.connect()

        try:
            data = obb.stocks.load(**params).to_df()
            data.reset_index(inplace=True)

        except Exception as e:
            log.logger.error(f'Error accessing data from OpenBB: {e}!')
            data = pd.DataFrame()
        
        return data
       
    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query, dialect='mindsdb')
        return self.query(ast)

    def call_openbb_api(self, method_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the OpenBB Platform method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call (e.g. klines)
            params (Dict): Params to pass to the API call
        """
        if method_name == 'stocks_load':
            return self._get_stocks_load(params)
        raise NotImplementedError('Method name {} not supported by OpenBB Platform Handler'.format(method_name))
