from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.financial_modeling_prep_handler.financial_modeling_tables import HistoricalPriceTable

from mindsdb.utilities import log
import requests

logger = log.getLogger(__name__)


class FinancialModelingHandler(APIHandler):

    name = "financial_modeling_prep"

    def __init__(self, name, connection_data: dict, **kwargs):
        super().__init__(name)

        self.api_key = None
        self.connection_data = connection_data
        if "api_key" not in connection_data:
            raise Exception(
                "FINANCIAL_MODELING engine requires an API key. Retrieve an API key from https://site.financialmodelingprep.com/developer. See financial_modeling_prep_handler/README.MD on how to include API key in query."
            )
        self.api_key = connection_data['api_key']
        self.client = None
        self.is_connected = False

        historical_prices = HistoricalPriceTable(self)
        self._register_table('historical_prices', historical_prices)

    def connect(self):
        self.is_connected = True
        base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
        return base_url

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        base_url = 'https://financialmodelingprep.com/api/v3/search'
        param = {
            'query': 'AA',
            'apikey': self.api_key,
            'limit': 5
        }

        response = requests.get(base_url, param)
        if response.status_code == 200:
            return StatusResponse(success=True)
        else:
            raise Exception(
                "API key provided in query is not valid. Retrieve a valid API key from https://site.financialmodelingprep.com/developer. See financial_modeling_prep_handler/README.MD on how to include API key in query."
            )
