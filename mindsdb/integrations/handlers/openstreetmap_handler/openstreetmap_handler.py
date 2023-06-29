import pandas as pd
from typing import Text, List, Dict

from mindsdb.integrations.handlers.openstreetmap_handler.openstreetmap_tables import ( OpenStreetMapNodeTable, OpenStreetMapWayTable, OpenStreetMapRelationTable )
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)


class OpenStreetMapHandler(APIHandler):
    """The OpenStreetMap handler implementation."""

    def __init__(self, name: str = None, **kwargs):
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

        nodes_data = NodesTable(self)
        self._register_table("nodes", nodes_data)

        ways_data = WaysTable(self)
        self._register_table("ways", ways_data)

        relations_data = RelationsTable(self)
        self._register_table("relations", relations_data)

    def connect(self):
        """Set up the connection required by the handler.

        Returns:
            StatusResponse: connection object
        """
        if self.is_connected is True:
            return self.connection

        api_session = overpy.Overpass()

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
        













    
    
    def get_table(self, table_name: str, **kwargs) -> List[Dict]:
        """Get data from the table.

        Args:
            table_name (str): The name of the table to get data from.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            List[Dict]: The data from the table.
        """
        if table_name == "nodes":
            return self.get_nodes(**kwargs)
        elif table_name == "ways":
            return self.get_ways(**kwargs)
        elif table_name == "relations":
            return self.get_relations(**kwargs)
        else:
            raise ValueError(f"Table {table_name} is not supported.")
        
    def get_table_as_pandas(self, table_name: str, **kwargs) -> pd.DataFrame:
        """Get data from the table as a pandas dataframe.

        Args:
            table_name (str): The name of the table to get data from.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            pd.DataFrame: The data from the table.
        """
        return pd.json_normalize(self.get_table(table_name, **kwargs))
    
    def get_table_as_mindsdb(self, table_name: str, **kwargs) -> DataFrame:
        """Get data from the table as a mindsdb dataframe.

        Args:
            table_name (str): The name of the table to get data from.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            DataFrame: The data from the table.
        """
        return DataFrame(self.get_table_as_pandas(table_name, **kwargs))