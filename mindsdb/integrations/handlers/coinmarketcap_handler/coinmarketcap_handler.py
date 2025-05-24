import requests
from typing import Dict, List
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log

from .coinmarketcap_tables import (
    ListingTable,
    QuotesTable,
    InfoTable,
    GlobalMetricsTable
)

logger = log.getLogger(__name__)

class CoinMarketCapHandler(APIHandler): # Inherits from APIHandler
    """ 
    The CoinMarketCap Handler implementation
    """

    def __init__(self, name: str, **kwargs):
        """ 
        Initialize the CoinMarketCap Handler
        """
        super().__init__(name)

        # Get connection data (API key, etc.)
        connection_data = kwargs.get('connection_data', {})
        self.connection_data = connection_data

        # API configuration
        self.api_key = connection_data.get('api_key')
        self.base_url = connection_data.get('base_url', 'https://pro-api.coinmarketcap.com')

        # Initialize tables
        self._register_table('listings', ListingTable(self))
        self._register_table('quotes', QuotesTable(self))
        self._register_table('info', InfoTable(self))
        self._register_table('global_metrics', GlobalMetricsTable(self))

        self.is_connected = False

    def connect(self) -> StatusResponse:
        """ 
        Set up the connection to CoinmarketCap API
        """
        if self.is_connected:
            return StatusResponse(
                status=RESPONSE_TYPE.SUCCESS,
                message='Already connected to CoinMarketCap API'
            )
        
        try:
            # make a simple test request
            headers = {
                'X-CMC_PRO_API_KEY': self.api_key,
                'Accepts': 'application/json'
            }

            response = requests.get(
                f'{self.base_url}/v1/cryptocurrency/listings/latest',
                headers=headers
                timeout=10
            )

            if response.status_code == 200:
                self.is_connected = True
                return StatusResponse(True)
            else:
                return StatusResponse(False, error_message=str(e))
        except Exception as e:
            return StatusResponse(False, error_message=f"API return status {response.status_code}")

    def disconnect(self):
        """ Close any existing connections

        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def check_connection(self) -> HandlerStatusResponse:
        """ Check connection to the handler

        Returns:
            HandlerStatusResponse
        """
        raise NotImplementedError()

    def native_query(self, query: Any) -> HandlerResponse:
        """Receive raw query and act upon it somehow.

        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def query(self, query: ASTNode) -> HandlerResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def get_tables(self) -> HandlerResponse:
        """ Return list of entities

        Return list of entities that will be accesible as tables.

        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        raise NotImplementedError()

    def get_columns(self, table_name: str) -> HandlerResponse:
        """ Returns a list of entity columns

        Args:
            table_name (str): name of one of tables returned by self.get_tables()

        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
        raise NotImplementedError()
