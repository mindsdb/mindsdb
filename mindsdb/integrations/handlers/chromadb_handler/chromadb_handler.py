from collections import OrderedDict

import chromadb
import pandas as pd
from chromadb import API
from chromadb.config import Settings
from langchain.vectorstores import Chroma

from mindsdb.integrations.handlers.chromadb_handler.helpers import (
    extract_collection_name,
)
from mindsdb.integrations.libs.base import VectorStoreHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log


class ChromaDBHandler(Chroma, VectorStoreHandler):
    """This handler handles connection and execution of the ChromaDB statements."""

    name = "chromadb"

    def __init__(self, name: str, **kwargs):

        self._client_settings = kwargs.get("connection_data")

        self._collection_name = self._client_settings.get("collection_name", "default")
        self._embedding_function = self._client_settings.get("embedding_function")
        self._persist_directory = self._client_settings.get("persist_directory")
        self._collection_metadata = self._client_settings.get("collection_metadata")

        self._client = chromadb.Client(Settings())
        self.is_connected = True

        VectorStoreHandler.__init__(self, name)

        Chroma.__init__(
            self,
            client=self._client,
            client_settings=self._client_settings,
            embedding_function=self._embedding_function,
            collection_name=self._collection_name,
            collection_metadata=self._collection_metadata,
            persist_directory=self._persist_directory,
        )

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> API:
        """Connect to a DuckDB database.

        Returns:
            API: The ChromaDB _client.
        """

        if self.is_connected is True:
            return self._client

        try:
            client_config = {
                "chroma_api_impl": self._client_settings.get("chroma_api_impl"),
                "chroma_server_host": self._client_settings.get("chroma_server_host"),
                "chroma_server_http_port": self._client_settings.get(
                    "chroma_server_http_port"
                ),
            }
            self._client = chromadb.Client(Settings(**client_config))
            self.is_connected = True
        except Exception as e:
            log.logger.error(f"Error connecting to ChromaDB client, {e}!")

        return self._client

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the ChromaDB database."""

        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._client.heartbeat()
            responseCode.success = True
        except Exception as e:
            log.logger.error(
                f'Error connecting to ChromaDB client {self._client_settings["chroma_server_host"]}, {e}!'
            )
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode

    def native_query(self, query: str) -> Response:
        """
        Execute a native query on the ChromaDB database.
        """
        try:
            self.connect()
            # parse query and extract collection name and any conditions
            collection_name = extract_collection_name(query)
            result = self._client.get_collection(collection_name)

        except Exception as e:
            log.logger.error(
                f'Error executing native query on ChromaDB client {self._client_settings["chroma_server_host"]}, {e}!'
            )
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f'Error executing native query on ChromaDB client {self._client_settings["chroma_server_host"]}, {e}!',
            )

        return Response(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=result,
        )

    def get_tables(self) -> Response:
        """Get the list of indexes/collections in the vectorDB.

        Returns:
           Response: The response object.
        """

        try:
            self.connect()
            collections = self._client.list_collections()
            collections_name = pd.DataFrame(
                columns=["table_name"],
                data=[collection.name for collection in collections],
            )
        except Exception as e:
            log.logger.error(
                f'Error getting tables from ChromaDB client {self._client_settings["chroma_server_host"]}, {e}!'
            )
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f'Error getting tables from ChromaDB client {self._client_settings["chroma_server_host"]}, {e}!',
            )

        return Response(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=collections_name,
        )


connection_args = OrderedDict(
    chroma_api_impl={
        "type": ARG_TYPE.STR,
        "description": "blah blah blah",
    },
    chroma_server_host={
        "type": ARG_TYPE.STR,
        "description": "blah blah blah",
    },
    chroma_server_http_port={
        "type": ARG_TYPE.INT,
        "description": "blah blah blah",
    },
)

connection_args_example = OrderedDict(
    chroma_api_impl="local", chroma_server_host="local", chroma_server_http_port=5432
)
