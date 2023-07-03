import overpy

import pandas as pd
from typing import Text, List, Dict

from mindsdb.integrations.handlers.openstreetmap_handler.openstreetmap_tables import ( OpenStreetMapNodeTable, 
    OpenStreetMapWayTable, OpenStreetMapRelationTable )
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)


class OpenStreetMapHandler(APIHandler):
    """The OpenStreetMap handler implementation."""

    def __init__(self, name: str, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        nodes_data = OpenStreetMapNodeTable(self)
        self._register_table("nodes", nodes_data)

        ways_data = OpenStreetMapWayTable(self)
        self._register_table("ways", ways_data)

        relations_data = OpenStreetMapRelationTable(self)
        self._register_table("relations", relations_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns:
            StatusResponse: connection object
        """
        if self.is_connected is True:
            return self.connection

        api_session = overpy.Overpass( **self.connection_data)

        self.connection = api_session

        self.is_connected = True

        return self.connection
    
    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns:
            HandlerStatusResponse
        """
        try:
            self.connect()
            return StatusResponse(status=True)
        except Exception as e:
            return StatusResponse(status=False, message=str(e))
        
    def native_query(self, query: Text) -> Response:
        """Execute a native query on the handler.

        Args:
            query (Text): The query to execute.

        Returns:
            Response: The response from the query.
        """
        try:
            self.connect()
            result = self.connection.query(query)
            return Response(status=True, data=result)
        except Exception as e:
            return Response(status=False, message=str(e))
        
    def call_openstreetmap_api(self, method_name:str = None, params:dict = None) -> pd.DataFrame:
        """Call the OpenStreetMap API.

        Args:
            method_name (str): The name of the API method to call.
            params (dict): The parameters to pass to the API method.

        Returns:
            pd.DataFrame: The response from the API.
        """
        if method_name is None:
            raise ValueError("method_name must be specified.")
        if params is None:
            params = {}
        self.connect()
        method = getattr(self.connection, method_name)
        result = method(**params)
        return pd.DataFrame(result)

