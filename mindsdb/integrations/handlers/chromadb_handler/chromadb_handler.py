from collections import OrderedDict

import chromadb
import pandas as pd
from chromadb import API
from chromadb.config import Settings
from integrations.handlers.chromadb_handler.settings import DEFAULT_EMBEDDINGS_MODEL
from langchain.vectorstores import Chroma
from mindsdb_sql import ASTNode, CreateTable, Insert, Select

from mindsdb.integrations.handlers.chromadb_handler.helpers import (
    extract_collection_name,
    load_embeddings_model,
    split_documents,
)
from mindsdb.integrations.libs.base import VectorStoreHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log

# todo create separate util dir for vectorstores


class ChromaDBHandler(Chroma, VectorStoreHandler):
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
        }

        self._collection_name = self._connection_data.get(
            "collection_name", "default_collection"
        )
        self._embedding_model_name = self._connection_data.get(
            "embedding_function", DEFAULT_EMBEDDINGS_MODEL
        )

        self._embedding_function = load_embeddings_model(self._embedding_model_name)

        self._persist_directory = self._connection_data.get("persist_directory")
        self._collection_metadata = self._connection_data.get("collection_metadata")

        self._client = chromadb.Client()
        self.is_connected = True

        VectorStoreHandler.__init__(self, name)

        Chroma.__init__(
            self,
            client=self._client,
            client_settings=self._connection_data,
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

            self._client = chromadb.Client(Settings(**self._client_config))
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

    def get_collection(self, query: ASTNode) -> Response:
        """
        Run a select query on the ChromaDB database, filter collection (if where clause)
        and return result. NB Limit will define the number of documents returned, not the number of rows.
        """

        collection_name = query.from_table.parts[-1]

        if query.where:
            # if there is a where clause, parse it and extract the conditions

            # todo add support for other operators
            # todo add support for WHERE IN

            where = {}
            if query.where.op == "and":

                for arg in query.where.args:
                    if arg.op == "=":
                        # todo add support for in operator
                        where[arg.args[0].parts[-1]] = arg.args[1].value
                    else:
                        raise NotImplementedError(
                            f"Unsupported where clause {arg.op} operator, only '=' is supported"
                        )

            elif query.where.op == "=":
                where[query.where.args[0].parts[-1]] = query.where.args[1].value

            else:
                raise NotImplementedError(
                    f"Unsupported where clause {query.where.op} operator, only '=' and 'and' is supported"
                )
        else:
            # if there is no where clause, set it to None
            where = None

        collection_data = self._client.get_collection(collection_name).get(
            where=where, include=["documents", "metadatas"]
        )

        result = pd.DataFrame(
            columns=["ids", "documents", "metadatas"], data=collection_data
        )

        if query.limit:
            # if there is a limit clause, limit the result
            result = result.head(query.limit.value)

        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result)

    def run_create_table(self, query: ASTNode) -> Response:
        """
        Run a create table query on the ChromaDB database.
        """
        # todo add support for adding data in the insert statement
        collection_name = query.name.parts[-1]
        self._client.create_collection(collection_name)

        return Response(resp_type=RESPONSE_TYPE.OK)

    def run_insert(self, query):
        """
        Run an insert query on the ChromaDB database.
        """
        collection_name = query.table.parts[-1]
        columns = [column.name for column in query.columns]
        df = pd.DataFrame(data=query.values, columns=columns)
        documents = split_documents(df, columns)

        # converts list of Documents to embedding vectors and stores them in a ChromaDB collection
        Chroma.from_documents(
            documents=documents,
            embedding=self._embedding_function,
            persist_directory=self._persist_directory,
            client_settings=Settings(**self._client_config),
            collection_name=collection_name,
        )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def query(self, query: ASTNode) -> Response:
        """
        Execute a query on the ChromaDB database.
        """
        try:
            self.connect()

            if isinstance(query, Select):
                return self.get_collection(query)

            elif isinstance(query, CreateTable):
                return self.run_create_table(query)

            elif isinstance(query, Insert):
                return self.run_insert(query)

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
                f'Error executing native query on ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!'
            )
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f'Error executing native query on ChromaDB client {self._connection_data["chroma_server_host"]}, {e}!',
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
