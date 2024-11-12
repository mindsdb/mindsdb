import ast
import sys
from typing import Dict, List, Optional, Union

import pandas as pd

from mindsdb.integrations.handlers.chromadb_handler.settings import ChromaHandlerConfig
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
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def get_chromadb():
    """
    Import and return the chromadb module, using pysqlite3 if available.
    this is a hack to make chromadb work with pysqlite3 instead of sqlite3 for cloud usage
    see https://docs.trychroma.com/troubleshooting#sqlite
    """

    # if we are using python 3.10 or above, we don't need pysqlite
    if sys.hexversion < 0x30A0000:
        try:
            __import__("pysqlite3")
            sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")
        except ImportError:
            logger.warn(
                "Python version < 3.10 and pysqlite3 is not installed. ChromaDB may not work without solving one of these: https://docs.trychroma.com/troubleshooting#sqlite"
            )  # noqa: E501

    try:
        import chromadb

        return chromadb
    except ImportError:
        raise ImportError("Failed to import chromadb.")


class ChromaDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the ChromaDB statements."""

    name = "chromadb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.handler_storage = HandlerStorage(kwargs.get("integration_id"))
        self._client = None
        self.persist_directory = None
        self.is_connected = False

        config = self.validate_connection_parameters(name, **kwargs)

        self._client_config = {
            "chroma_server_host": config.host,
            "chroma_server_http_port": config.port,
            "persist_directory": self.persist_directory,
        }

        self.connect()

    def validate_connection_parameters(self, name, **kwargs):
        """
        Validate the connection parameters.
        """

        _config = kwargs.get("connection_data")
        _config["vector_store"] = name

        config = ChromaHandlerConfig(**_config)

        if config.persist_directory and not self.handler_storage.is_temporal:
            # get full persistence directory from handler storage
            self.persist_directory = self.handler_storage.folder_get(
                config.persist_directory
            )

        return config

    def _get_client(self):
        client_config = self._client_config
        if client_config is None:
            raise Exception("Client config is not set!")

        chromadb = get_chromadb()

        # decide the client type to be used, either persistent or httpclient
        if client_config["persist_directory"] is not None:
            return chromadb.PersistentClient(path=client_config["persist_directory"])
        else:
            return chromadb.HttpClient(
                host=client_config["chroma_server_host"],
                port=client_config["chroma_server_http_port"],
            )

    def _sync(self):
        # if handler storage is used: sync on every change write operation
        if self.persist_directory:
            self.handler_storage.folder_sync(self.persist_directory)

    def __del__(self):
        """Close the database connection."""

        if self.is_connected is True:
            self._sync()

            self.disconnect()

    def connect(self):
        """Connect to a ChromaDB database."""
        if self.is_connected is True:
            return self._client

        try:
            self._client = self._get_client()
            self.is_connected = True
            return self._client
        except Exception as e:
            self.is_connected = False
            raise Exception(f"Error connecting to ChromaDB client, {e}!")

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the ChromaDB database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._client.heartbeat()
            response_code.success = True
        except Exception as e:
            logger.error(f"Error connecting to ChromaDB , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False

        return response_code

    def _get_chromadb_operator(self, operator: FilterOperator) -> str:
        mapping = {
            FilterOperator.EQUAL: "$eq",
            FilterOperator.NOT_EQUAL: "$ne",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$lte",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by ChromaDB!")

        return mapping[operator]

    def _translate_metadata_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by ChromaDB.
        E.g.,
        [
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.LESS_THAN,
                value="2020-01-01",
            ),
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.GREATER_THAN,
                value="2019-01-01",
            )
        ]
        -->
        {
            "$and": [
                {"created_at": {"$lt": "2020-01-01"}},
                {"created_at": {"$gt": "2019-01-01"}}
            ]
        }
        """
        # we ignore all non-metadata conditions
        if conditions is None:
            return None
        metadata_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        if len(metadata_conditions) == 0:
            return None

        # we translate each metadata condition into a dict
        chroma_db_conditions = []
        for condition in metadata_conditions:
            metadata_key = condition.column.split(".")[-1]
            chroma_db_conditions.append(
                {
                    metadata_key: {
                        self._get_chromadb_operator(condition.op): condition.value
                    }
                }
            )

        # we combine all metadata conditions into a single dict
        metadata_condition = (
            {"$and": chroma_db_conditions}
            if len(chroma_db_conditions) > 1
            else chroma_db_conditions[0]
        )
        return metadata_condition

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        collection = self._client.get_collection(table_name)
        filters = self._translate_metadata_condition(conditions)

        include = ["metadatas", "documents", "embeddings"]

        # check if embedding vector filter is present
        vector_filter = (
            []
            if conditions is None
            else [
                condition
                for condition in conditions
                if condition.column == TableField.EMBEDDINGS.value
            ]
        )

        if len(vector_filter) > 0:
            vector_filter = vector_filter[0]
        else:
            vector_filter = None
        id_filters = []
        if conditions is not None:
            for condition in conditions:
                if condition.column != TableField.ID.value:
                    continue
                if condition.op == FilterOperator.EQUAL:
                    id_filters.append(condition.value)
                elif condition.op == FilterOperator.IN:
                    id_filters.extend(condition.value)

        if vector_filter is not None:
            # similarity search
            query_payload = {
                "where": filters,
                "query_embeddings": vector_filter.value
                if vector_filter is not None
                else None,
                "include": include + ["distances"],
            }
            if limit is not None:
                query_payload["n_results"] = limit

            result = collection.query(**query_payload)
            ids = result["ids"][0]
            documents = result["documents"][0]
            metadatas = result["metadatas"][0]
            distances = result["distances"][0]
            embeddings = result["embeddings"][0]

        else:
            # general get query
            result = collection.get(
                ids=id_filters,
                where=filters,
                limit=limit,
                offset=offset,
                include=include,
            )
            ids = result["ids"]
            documents = result["documents"]
            metadatas = result["metadatas"]
            embeddings = result["embeddings"]
            distances = None

        # project based on columns
        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: documents,
            TableField.METADATA.value: metadatas,
            TableField.EMBEDDINGS.value: embeddings,
        }

        if columns is not None:
            payload = {column: payload[column] for column in columns}

        # always include distance
        if distances is not None:
            payload[TableField.DISTANCE.value] = distances
        return pd.DataFrame(payload)

    def insert(self, table_name: str, data: pd.DataFrame):
        """
        Insert data into the ChromaDB database.
        """

        collection = self._client.get_or_create_collection(name=table_name)

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        def dataframe_metadata_to_chroma_metadata(metadata: Union[Dict[str, str], str]) -> Optional[Dict[str, str]]:
            if isinstance(metadata, dict):
                if not metadata:
                    # ChromaDB does not support empty metadata dicts, but it does support None.
                    # Related: https://github.com/chroma-core/chroma/issues/791.
                    return None
                return metadata
            # Metadata is a string representation of a dictionary instead.
            return ast.literal_eval(metadata)

        # ensure metadata is a dict, convert to dict if it is a string
        if data.get(TableField.METADATA.value) is not None:
            data[TableField.METADATA.value] = data[TableField.METADATA.value].apply(dataframe_metadata_to_chroma_metadata)

        # convert to dict

        data = data.to_dict(orient="list")

        collection.upsert(
            ids=data[TableField.ID.value],
            documents=data.get(TableField.CONTENT.value),
            embeddings=data[TableField.EMBEDDINGS.value],
            metadatas=data.get(TableField.METADATA.value),
        )
        self._sync()

    def upsert(self, table_name: str, data: pd.DataFrame):
        return self.insert(table_name, data)

    def update(
        self,
        table_name: str,
        data: pd.DataFrame,
        key_columns: List[str] = None,
    ):
        """
        Update data in the ChromaDB database.
        """
        collection = self._client.get_collection(table_name)

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        data = data.to_dict(orient="list")

        collection.update(
            ids=data[TableField.ID.value],
            documents=data.get(TableField.CONTENT.value),
            embeddings=data[TableField.EMBEDDINGS.value],
            metadatas=data.get(TableField.METADATA.value),
        )
        self._sync()

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        filters = self._translate_metadata_condition(conditions)
        # get id filters
        id_filters = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ] or None

        if filters is None and id_filters is None:
            raise Exception("Delete query must have at least one condition!")
        collection = self._client.get_collection(table_name)
        collection.delete(ids=id_filters, where=filters)
        self._sync()

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Create a collection with the given name in the ChromaDB database.
        """
        self._client.create_collection(table_name, get_or_create=if_not_exists)
        self._sync()

    def drop_table(self, table_name: str, if_exists=True):
        """
        Delete a collection from the ChromaDB database.
        """
        try:
            self._client.delete_collection(table_name)
            self._sync()
        except ValueError:
            if if_exists:
                return
            else:
                raise Exception(f"Collection {table_name} does not exist!")

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of collections in the ChromaDB database.
        """
        collections = self._client.list_collections()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=[collection.name for collection in collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        # check if collection exists
        try:
            _ = self._client.get_collection(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return super().get_columns(table_name)
