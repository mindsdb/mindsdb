from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.aqicn_handler.aqicn_tables import (
    AQByUserLocationTable,
    AQByCityTable,
    AQByLatLngTable,
    AQByNetworkStationTable
)
from mindsdb.integrations.handlers.aqicn_handler.aqicn import AQIClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class AQICNHandler(APIHandler):
    """The World Air Quality handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the aqicn handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.aqicn_client = None
        self.is_connected = False

        ai_user_location_data = AQByUserLocationTable(self)
        self._register_table("air_quality_user_location", ai_user_location_data)

        ai_city_data = AQByCityTable(self)
        self._register_table("air_quality_city", ai_city_data)

        ai_lat_lng_data = AQByLatLngTable(self)
        self._register_table("air_quality_lat_lng", ai_lat_lng_data)

        aq_network_station_data = AQByNetworkStationTable(self)
        self._register_table("air_quality_station_by_name", aq_network_station_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        self.aqicn_client = AQIClient(self.connection_data.get("api_key"))
        content = self.aqicn_client.air_quality_user_location()
        if content["code"] != 200:
            resp.success = False
            resp.error_message = content["content"]["data"]
            self.is_connected = False
            return resp
        self.is_connected = True
        resp.success = True
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = self.connect()
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
