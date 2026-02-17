from mindsdb.integrations.handlers.oilpriceapi_handler.oilpriceapi_tables import (
    OilPriceLatestTable,
    OilPricePastDayPriceTable
)
from mindsdb.integrations.handlers.oilpriceapi_handler.oilpriceapi import OilPriceAPIClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql


logger = log.getLogger(__name__)


class OilPriceAPIHandler(APIHandler):
    """The OilPriceAPI handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the OilPriceAPI handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.client = OilPriceAPIClient(self.connection_data["api_key"])
        self.is_connected = False

        latest_price_data = OilPriceLatestTable(self)
        self._register_table("latest_price", latest_price_data)

        past_day_price_data = OilPricePastDayPriceTable(self)
        self._register_table("past_day_price", past_day_price_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        status = self.client.get_latest_price()
        if status["code"] != 200:
            resp.success = False
            resp.error_message = status["error"]
            return resp
        self.is_connected = True
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)

        try:
            status = self.client.get_latest_price()
            if status["code"] == 200:
                logger.info("Authentication successful")
                response.success = True
            else:
                response.success = False
                logger.info("Error connecting to OilPriceAPI. " + status["error"])
                response.error_message = status["error"]
        except Exception as e:
            logger.error(f"Error connecting to OilPriceAPI: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str) -> StatusResponse:
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
        ast = parse_sql(query)
        return self.query(ast)
