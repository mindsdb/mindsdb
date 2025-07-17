from mindsdb.integrations.handlers.zipcodebase_handler.zipcodebase_tables import (
    ZipCodeBaseCodeLocationTable,
    ZipCodeBaseCodeInRadiusTable,
    ZipCodeBaseCodeByCityTable,
    ZipCodeBaseCodeByStateTable,
    ZipCodeBaseStatesByCountryTable
)
from mindsdb.integrations.handlers.zipcodebase_handler.zipcodebase import ZipCodeBaseClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql


logger = log.getLogger(__name__)


class ZipCodeBaseHandler(APIHandler):
    """The ZipCodeBase handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the ZipCodeBase handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.client = ZipCodeBaseClient(self.connection_data["api_key"])
        self.is_connected = False

        code_to_location_data = ZipCodeBaseCodeLocationTable(self)
        self._register_table("code_to_location", code_to_location_data)

        codes_within_radius_data = ZipCodeBaseCodeInRadiusTable(self)
        self._register_table("codes_within_radius", codes_within_radius_data)

        codes_by_city_data = ZipCodeBaseCodeByCityTable(self)
        self._register_table("codes_by_city", codes_by_city_data)

        codes_by_state_data = ZipCodeBaseCodeByStateTable(self)
        self._register_table("codes_by_state", codes_by_state_data)

        states_by_country_data = ZipCodeBaseStatesByCountryTable(self)
        self._register_table("states_by_country", states_by_country_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        status = self.client.remaining_requests()
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
            status = self.client.remaining_requests()
            if status["code"] == 200:
                logger.info("Authentication successful")
                response.success = True
            else:
                response.success = False
                logger.info("Error connecting to ZipCodeBase. " + status["error"])
                response.error_message = status["error"]
        except Exception as e:
            logger.error(f"Error connecting to ZipCodeBase: {e}!")
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
