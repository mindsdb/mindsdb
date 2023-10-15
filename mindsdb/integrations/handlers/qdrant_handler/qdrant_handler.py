from collections import OrderedDict
from typing import List, Optional
from itertools import zip_longest

from qdrant_client import QdrantClient, models
import pandas as pd

from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log


class QdrantHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Qdrant statements."""

    name = "qdrant"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        connection_data = kwargs.get("connection_data").copy()
        self.collection_config = connection_data.pop("collection_config")
        self.connect(**connection_data)

    def connect(self, **kwargs):
        """Connect to a Qdrant instance."""
        if self.is_connected:
            return self._client

        try:
            self._client = QdrantClient(**kwargs)
            self.is_connected = True
            return self._client
        except Exception as e:
            log.logger.error(f"Error instantiating a Qdrant client: {e}")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected:
            self._client.close()
            self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Qdrant database."""
        need_to_close = not self.is_connected

        try:
            self._client.get_locks()
            response_code = StatusResponse(True)
        except Exception as e:
            log.logger.error(f"Error connecting to a Qdrant instance: {e}")
            response_code = StatusResponse(False, error_message=str(e))
        finally:
            if response_code.success and need_to_close:
                self.disconnect()
            if not response_code.success and self.is_connected:
                self.is_connected = False

        return response_code

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """
        Delete a collection from the Qdrant Instance.
        """
        result = self._client.delete_collection(table_name)
        if result or if_exists:
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

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Insert data into the Qdrant instance.
        """
        assert len(data[TableField.ID.value]) == len(data[TableField.EMBEDDINGS.value]), "Number of ids and embeddings must be equal"

        data = data.to_dict(orient="list")
        payloads = []
        content_list = data[TableField.CONTENT.value]
        metadata_list = data[TableField.METADATA.value]

        for document, metadata in zip_longest(content_list, metadata_list, fillvalue=None):
            payload = {}

            if document is not None:
                payload["document"] = document

            if metadata is not None:
                payload = {**payload, **metadata}

            if payload:
                payloads.append(payload)

        # convert ids to int if numeric else leave as is if string(UUID)
        ids = [int(id) if str(id).isdigit() else id for id in data[TableField.ID.value]]
        self._client.upsert(table_name, points=models.Batch(
            ids=ids,
            vectors=data[TableField.EMBEDDINGS.value],
            payloads=payloads
        ))

        return Response(resp_type=RESPONSE_TYPE.OK)

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """
        Create a collection with the given name in the Qdrant database.
        """
        try:
            self._client.create_collection(table_name, self.collection_config)
        except ValueError:
            if not if_not_exists:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message=f"Table {table_name} already exists!",
                )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def select(self, table_name: str, columns: Optional[List[str]] = None, conditions: Optional[List[FilterCondition]] = None, offset: int = 0, limit: int = 10,) -> HandlerResponse:

        # Constants and defaults
        DEFAULT_OFFSET = 0
        DEFAULT_LIMIT = 10

        # Validate and set offset and limit as None is passed if not set in the query
        offset = offset if offset is not None else DEFAULT_OFFSET
        limit = limit if limit is not None else DEFAULT_LIMIT

        # Full scroll if no where conditions are specified
        if not conditions:
            results = self._client.scroll(table_name, limit=limit, offset=offset)
            payload = self._process_select_results(results[0], columns)
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=payload)

        # Filter conditions
        vector_filter = [condition.value for condition in conditions if condition.column == TableField.SEARCH_VECTOR.value]
        id_filters = [condition.value for condition in conditions if condition.column == TableField.ID.value]
        query_filters = []

        if id_filters:
            results = self._client.retrieve(table_name, ids=id_filters)
        elif vector_filter:
            results = self._client.search(table_name, query_vector=vector_filter, limit=limit, offset=offset)
        elif query_filters:
            raise NotImplementedError("Query scroll is not implemented yet")

        # Process results
        payload = self._process_select_results(results, columns)
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=payload)

    def _process_select_results(self, results, columns):
        ids, documents, metadata, distances = [], [], [], []

        for result in results:
            ids.append(result.id)
            documents.append(result.payload["document"])
            metadata.append({k: v for k, v in result.payload.items() if k != "document"})

            # Score is only available for similarity search results
            if "score" in result:
                distances.append(result.score)

        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: documents,
            TableField.METADATA.value: metadata,
        }

        # Filter result columns
        if columns:
            payload = {
                column: payload[column]
                for column in columns
                if column != TableField.EMBEDDINGS.value and column in payload
            }

        # If the distance list is empty, don't add it to the result
        if distances:
            payload[TableField.DISTANCE.value] = distances

        return pd.DataFrame(payload)


connection_args = OrderedDict(
    location={
        "type": ARG_TYPE.STR,
        "description": "If `:memory:` - use in-memory Qdrant instance. If a remote URL - connect to a remote Qdrant instance. Example: `http://localhost:6333`",
        "required": False,
    },
    url={
        "type": ARG_TYPE.STR,
        "description": "URL of Qdrant service. Either host or a string of type [scheme]<host><[port][prefix]. Ex: http://localhost:6333/service/v1",
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
    collection_config={
        "type": ARG_TYPE.DICT,
        "description": "Collection creation configuration. See https://qdrant.github.io/qdrant/redoc/index.html#tag/collections/operation/create_collection",
        "required": True,
    },
)

connection_args_example = {
    "location": ":memory:",
    "collection_config": {
        "size": 386,
        "distance": "Cosine"
    }
}
