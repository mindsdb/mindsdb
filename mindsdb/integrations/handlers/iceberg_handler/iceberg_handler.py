from typing import Optional

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler

from pyiceberg.catalog import load_catalog

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)

class IcebergHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Apache-Iceberg statements.
    """

    name = "iceberg"

    def __init__(self, name: str, connection_data: Optional[dict] ,**kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "iceberg"
        self.connection_data = connection_data
        self.name = name

        self.uri = self.connection_data.get("uri")

        self.kwargs = kwargs
        self.is_connected = False
        self.connection = None

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        if self.is_connected is True:
            return self.connection
        
        try:
            catalog = load_catalog(
                self.name,
                **{
                    "type": "sql",
                    "uri": self.uri,
                }
            )

            self.catalog = catalog
            self.is_connected = True

            if "namespace" in self.connection_data:
                self.catalog.create_namespace(self.connection_data["namespace"])

            return self.catalog

        except Exception as e:
            raise e
    