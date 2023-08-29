import ast
import uuid
from collections import OrderedDict

import chromadb
import numpy as np
import pandas as pd
from chromadb import API
from chromadb.config import Settings
from mindsdb_sql import ASTNode, CreateTable, DropTables, Insert, Select, Star

from mindsdb.integrations.libs.base import VectorStoreHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log


class ChromaDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the ChromaDB statements."""

    name = "chromadb"

    def __init__(self, name: str, **kwargs):

        self._connection_data = kwargs.get("connection_data")

        self._client_config = self._client_config = {
            "chroma_api_impl": self._connection_data.get("chroma_api_impl"),
            "chroma_server_host": self._connection_data.get("chroma_server_host"),
            "chroma_server_http_port": self._connection_data.get(
                "chroma_server_http_port"
            ),
            "persist_directory": self._connection_data.get(
                "persist_directory", "chroma"
            ),
        }

        # self._client = chromadb.Client(Settings(**self._client_config))
        self._client = chromadb.PersistentClient()
        self.is_connected = True

        VectorStoreHandler.__init__(self, name)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> API:
        """Connect to a ChromaDB database.

        Returns:
            API: The ChromaDB _client.
        """

        if self.is_connected is True:
            return self._client

        try:
            # self._client = chromadb.Client(Settings(**self._client_config))
            self._client = chromadb.PersistentClient()
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
            log.logger.error(f"Error connecting to ChromaDB , {e}!")
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode

    def count(self, query: ASTNode):

        # if count is used, return the count of the collection
        if not isinstance(query.targets[0].args[0], Star):
            # if count is not using '*' argument, raise error
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message="Count only supports '*' argument",
            )

        collection_data = self._collection.count()
        return pd.DataFrame(columns=["count"], data=[[collection_data]])

    def similarity_search(self, query: ASTNode) -> Response:
        """
        Run a query on a chroma database
        """

        collection_name = query.from_table.parts[-1]
        self._collection = self._client.get_collection(collection_name)

        if hasattr(query.targets[0], "op") and query.targets[0].op == "count":
            # get count of embeddings in a collection

            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=self.count(query))

        elif isinstance(query.targets[0], Star):

            result = pd.DataFrame()

            if not query.where:
                # return data for select * without where - NB max results is 10
                collection_data = self._collection.peek(
                    limit=query.limit.value if query.limit else 10
                )
                result["id"] = collection_data["ids"]
                result["documents"] = collection_data["documents"]
                result["embeddings"] = collection_data["embeddings"]

            elif query.where.args[0].parts[-1] == "search_embedding":
                # return data based on similarity search from input embeddings
                search_embedding = query.where.args[1].items[0].value

                collection_data = self._collection.query(
                    query_embeddings=search_embedding,
                    n_results=query.limit.value if query.limit else 5,
                    include=["embeddings", "distances", "documents"],
                )

                embeddings_arr = np.array(collection_data["embeddings"][0])
                distance_arr = np.array(collection_data["distances"][0])

                # indices = np.where(distance_arr > 0.5)[0]
                result["id"] = collection_data["ids"][0]
                result["documents"] = collection_data["documents"][0]
                result["embeddings"] = embeddings_arr.tolist()
                result["distances"] = distance_arr.tolist()

            else:

                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message="SELECT only supports COUNT(*), SELECT * or WHERE with 'search_embeddings' parameter",
                )

        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result)

    def create_or_get_collection(self, query: ASTNode) -> Response:
        """
        Run a create table query on the ChromaDB database.
        """
        collection_name = query.name.parts[-1]
        self._client.get_or_create_collection(collection_name)

        return Response(resp_type=RESPONSE_TYPE.OK)

    def add_embeddings(self, query: Insert):
        """
        Run an insert query on the ChromaDB database.
        """
        collection_name = query.table.parts[-1]
        columns = [column.name for column in query.columns]

        # supported columns name are
        # "embeddings", "content", "id"
        if not set(columns).issubset({"embeddings", "content", "id"}):
            raise Exception(
                "Only 'embeddings', 'content' and 'id' columns are supported!"
            )

        # get id column if it is present
        if "id" in columns:
            id_col_index = columns.index("id")
            ids = [row[id_col_index] for row in query.values]
        else:
            ids = [str(uuid.uuid1()) for _ in query.values]

        # get content column if it is present
        if "content" in columns:
            content_col_index = columns.index("content")
            content = [row[content_col_index] for row in query.values]
        else:
            content = [None for _ in query.values]

        # get embeddings column if it is present
        if "embeddings" in columns:
            embeddings_col_index = columns.index("embeddings")
            embeddings = [
                ast.literal_eval(row[embeddings_col_index]) for row in query.values
            ]
        else:
            raise Exception("Embeddings column is required!")

        # Add new embeddings to Chroma collection
        self._client.get_collection(collection_name).add(
            ids=ids,
            documents=content,
            embeddings=embeddings,
        )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def drop_table(self, query: DropTables) -> Response:
        """
        Delete a table from the ChromaDB database.
        """
        collection_name = query.name.parts[-1]
        # check if collection exists
        if collection_name not in self._client.list_collections():
            raise Exception(f"Collection {collection_name} does not exist!")

        # delete collection
        self._client.delete_collection(collection_name)

        return Response(resp_type=RESPONSE_TYPE.OK)

    def query(self, query: ASTNode) -> Response:
        """
        Execute a query on the ChromaDB database.
        """
        try:
            self.connect()

            if isinstance(query, Select):
                return self.similarity_search(query)

            elif isinstance(query, CreateTable):
                return self.create_or_get_collection(query)

            elif isinstance(query, Insert):
                return self.add_embeddings(query)

            elif isinstance(query, DropTables):
                return self.delete(query)

            else:
                raise NotImplementedError(
                    f"Unsupported query type {query.__class__.__name__}!"
                )

        except Exception as e:
            log.logger.error(f"Error executing query on ChromaDB client, {e}!")
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error executing query on ChromaDB client, {e}!",
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
                f'Error getting tables from ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!'
            )
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f'Error getting tables from ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!',
            )

        return Response(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=collections_name,
        )

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get the list of columns in the vectorDB.

        Args:
            table_name (str): The name of the table.

        Returns:
            Response: The response object.
        """

        try:
            self.connect()
            _ = self._client.get_collection(table_name)
            columns = pd.DataFrame(
                columns=["column_name", "data_type"],
                data=[
                    ["id", "uuid"],
                    ["content", "str"],
                    ["embeddings", "list"],
                ],
            )
        except Exception as e:
            log.logger.error(
                f'Error getting columns from ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!'
            )
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f'Error getting columns from ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!',
            )

        return Response(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=columns,
        )


connection_args = OrderedDict(
    chroma_api_impl={
        "type": ARG_TYPE.STR,
        "description": "chromadb api implementation",
    },
    chroma_server_host={
        "type": ARG_TYPE.STR,
        "description": "chromadb server host",
    },
    chroma_server_http_port={
        "type": ARG_TYPE.INT,
        "description": "chromadb server port",
    },
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "persistence directory for chroma",
    },
)

connection_args_example = OrderedDict(
    chroma_api_impl="rest",
    chroma_server_host="localhost",
    chroma_server_http_port=8000,
    persist_directoryn="chroma",
)
