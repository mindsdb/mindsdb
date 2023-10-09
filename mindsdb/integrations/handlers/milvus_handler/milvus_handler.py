from collections import OrderedDict
from typing import List, Optional

import pandas as pd
from mindsdb.integrations.libs.const import \
    HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import \
    HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition, FilterOperator, TableField, VectorStoreHandler)
from mindsdb.utilities import log
from pymilvus import Collection, connections, utility


class MilvusHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Milvus statements."""

    name = "milvus"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs["connection_data"]
        # Extract parameters used while searching and leave the rest for establishing connection
        search_param_names = {
            "search_metric_type": "metric_type",
            "search_ignore_growing": "ignore_growing",
            "search_params": "params"
        }
        self._search_params = {}
        for search_param_alias, actual_search_param_name in search_param_names.items():
            if search_param_alias in self._connection_data:
                self._search_params[actual_search_param_name] = self._connection_data[search_param_alias]
                del self._connection_data[search_param_alias]
        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """Connect to a Milvus database."""
        if self.is_connected is True:
            return
        try:
            connections.connect(**self._connection_data)
            self.is_connected = True
        except Exception as e:
            log.logger.error(f"Error connecting to Milvus client: {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected is False:
            return
        connections.disconnect(self._connection_data["alias"])
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Milvus database."""
        response_code = StatusResponse(False)
        try:
            response_code.success = connections.has_connection(
                self._connection_data["alias"])
        except Exception as e:
            log.logger.error(f"Error checking Milvus connection: {e}!")
            response_code.error_message = str(e)
        return response_code

    def get_tables(self) -> HandlerResponse:
        """Get the list of collections in the Milvus database."""
        collections = utility.list_collections()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=[collection for collection in collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """Delete a collection from the Milvus database."""
        try:
            utility.drop_collection(table_name)
        except Exception as e:
            if if_exists:
                return Response(resp_type=RESPONSE_TYPE.OK)
            else:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message=f"Error dropping table '{table_name}': {e}",
                )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> HandlerResponse:

        # Load collection table
        collection = Collection(table_name)
        try:
            collection.load()
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error loading collection {table_name}: {e}",
            )
        # Find vector filter in conditions
        vector_filter = (
            []
            if conditions is None
            else [
                condition
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )
        # NOTE: According to api offset and limit should be less than 16384.
        api_limit = 16384
        if limit is not None and offset is not None and limit + offset >= api_limit:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Sum of limit and offset should be less than {api_limit}",
            )

        search_arguments = {
            "data": [vector_filter],  # search vector
            # name of the field (vector) to search on
            "anns_field": TableField.EMBEDDINGS.value,
            "param": self._search_params,
            "expr": None,  # boolean filters
            # cols (in metadata for mindsdb, table for milvus) to return
            "output_fields": ['title'],
        }
        if limit is not None:
            search_arguments["limit"] = limit
        if offset is not None:
            search_arguments["param"]["offset"] = offset

        results = collection.search(**search_arguments)

        """
        # get the IDs of all returned hits
        results[0].ids
        # get the distances to the query vector from all returned hits
        results[0].distances
        # get the value of an output field specified in the search request.
        hit = results[0][0]
        hit.entity.get('title')
        """

        collection = self._client.get_collection(table_name)
        filters = self._translate_metadata_condition(conditions)
        # check if embedding vector filter is present
        vector_filter = (
            []
            if conditions is None
            else [
                condition
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )
        if len(vector_filter) > 0:
            vector_filter = vector_filter[0]
        else:
            vector_filter = None
        id_filters = None
        if conditions is not None:
            id_filters = [
                condition.value
                for condition in conditions
                if condition.column == TableField.ID.value
            ] or None

        if vector_filter is not None:
            # similarity search
            query_payload = {
                "where": filters,
                "query_embeddings": vector_filter.value
                if vector_filter is not None
                else None,
                "include": ["metadatas", "documents", "distances"],
            }
            if limit is not None:
                query_payload["n_results"] = limit

            result = collection.query(**query_payload)
            ids = result["ids"][0]
            documents = result["documents"][0]
            metadatas = result["metadatas"][0]
            distances = result["distances"][0]
        else:
            # general get query
            result = collection.get(
                ids=id_filters,
                where=filters,
                limit=limit,
                offset=offset,
            )
            ids = result["ids"]
            documents = result["documents"]
            metadatas = result["metadatas"]
            distances = None

        # project based on columns
        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: documents,
            TableField.METADATA.value: metadatas,
        }

        if columns is not None:
            payload = {
                column: payload[column]
                for column in columns
                if column != TableField.EMBEDDINGS.value
            }

        # always include distance
        if distances is not None:
            payload[TableField.DISTANCE.value] = distances
        result_df = pd.DataFrame(payload)
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result_df)

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
            raise Exception(f"Operator {operator} is not supported by Milvus!")

        return mapping[operator]

    def _translate_metadata_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by Milvus.
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

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Insert data into the Milvus database.
        """

        collection = self._client.get_collection(table_name)

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        data = data.to_dict(orient="list")

        collection.add(
            ids=data[TableField.ID.value],
            documents=data.get(TableField.CONTENT.value),
            embeddings=data[TableField.EMBEDDINGS.value],
            metadatas=data.get(TableField.METADATA.value),
        )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Update data in the Milvus database.
        TODO: not implemented yet
        """
        return super().update(table_name, data, columns)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ) -> HandlerResponse:
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
        return Response(resp_type=RESPONSE_TYPE.OK)

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """Create a collection with the given name in the Milvus database."""
        self._client.create_collection(table_name, get_or_create=if_not_exists)
        return Response(resp_type=RESPONSE_TYPE.OK)

    def get_columns(self, table_name: str) -> HandlerResponse:
        # check if collection exists
        try:
            _ = self._client.get_collection(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        """
        data = pd.DataFrame(self.SCHEMA)
        data.columns = ["COLUMN_NAME", "DATA_TYPE"]
        return HandlerResponse(
            data_frame=data,
        )
        """
        return super().get_columns(table_name)


connection_args = OrderedDict(
    chroma_server_host={
        "type": ARG_TYPE.STR,
        "description": "chromadb server host",
        "required": False,
    },
    chroma_server_http_port={
        "type": ARG_TYPE.INT,
        "description": "chromadb server port",
        "required": False,
    },
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "persistence directory for chroma",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    chroma_server_host="localhost",
    chroma_server_http_port=8000,
    persist_directoryn="chroma",
)
