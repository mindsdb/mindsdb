from collections import OrderedDict
from typing import List, Optional

from qdrant_client import QdrantClient
import pandas as pd

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log


class QdrantHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Qdrant statements."""

    name = "qdrant"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connect(**kwargs.get("connection_data"))

    def connect(self, **kwargs):
        """Connect to a Qdrant instance."""
        if self.is_connected is True:
            return self._client

        try:
            self._client = QdrantClient(**kwargs)
            self.is_connected = True
            return self._client
        except Exception as e:
            log.logger.error(f"Error connecting to a Qdrant instance, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self._client.close()
        self._client = None

        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def check_connection(self):
        """Check the connection to the Qdrant database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._client.get_locks()
            response_code.success = True
        except Exception as e:
            log.logger.error(f"Error connecting to a Qdrant instance , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False

        return response_code

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """
        Delete a collection from the Qdrant Instance.
        """
        result = self._client.delete_collection(table_name)
        if result is True or if_exists is True:
            return Response(resp_type=RESPONSE_TYPE.OK)
        else:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of collections in the Qdrant instance.
        """
        collection_response = self._client.get_collections()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=[collection.name for collection in collection_response.collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        try:
            _ = self._client.get_collection(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return super().get_columns(table_name)


connection_args = OrderedDict(
    location={
        "type": ARG_TYPE.STR,
        "description": "If `:memory:` - use in-memory Qdrant instance. If a remote URL - connect to a remote Qdrant instance. Example: `http://localhost:6333`",
        "required": False,
    },
    url={
        "type": ARG_TYPE.STR,
        "description": "URL of Qdrant service. either host or a string of type [scheme]<host><[port][prefix]. Ex: http://localhost:6333/service/v1",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "Host name of Qdrant service. The port and host are used to construct the connection URL.",
        "required": False,
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Port of the REST API interface. Default: 6333",
        "required": False,
    },
    grpc_port={
        "type": ARG_TYPE.INT,
        "description": "Port of the gRPC interface. Default: 6334",
        "required": False,
    },
    prefer_grpc={
        "type": ARG_TYPE.BOOL,
        "description": "If `true` - use gPRC interface whenever possible in custom methods. Default: false",
        "required": False,
    },
    https={
        "type": ARG_TYPE.BOOL,
        "description": "If `true` - use https protocol.",
        "required": False,
    },
    api_key={
        "type": ARG_TYPE.STR,
        "description": "API key for authentication in Qdrant Cloud.",
        "required": False,
    },
    prefix={
        "type": ARG_TYPE.STR,
        "description": "If set, the value is added to the REST URL path. Example: `service/v1` will result in `http://localhost:6333/service/v1/{qdrant-endpoint}` for REST API",
        "required": False,
    },
    timeout={
        "type": ARG_TYPE.INT,
        "description": "Timeout for REST and gRPC API requests. Defaults to 5.0 seconds for REST and unlimited for gRPC",
        "required": False,
    },
    path={
        "type": ARG_TYPE.STR,
        "description": "Persistence path for a local Qdrant instance(:memory:).",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    location=":memory:",
)
