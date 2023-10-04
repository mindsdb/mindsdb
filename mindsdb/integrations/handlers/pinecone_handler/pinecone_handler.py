from collections import OrderedDict
from typing import List, Optional

import pinecone
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


class PineconeHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Pinecone statements."""

    name = "pinecone"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs.get("connection_data")
        self._client_config = {
            "api_key": self._connection_data.get("api_key"),
            "environment": self._connection_data.get("environment")
        }
        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _get_index_handle(self, index_name):
        """Returns handler to index specified by `index_name`"""
        index = pinecone.Index(index_name)
        try:
            pinecone.describe_index_stats()
        except:
            index = None
        return index

    def _get_pinecone_operator(self, operator: FilterOperator) -> str:
        """Convert FilterOperator to an operator that pinecone's query language can undersand"""
        mapping = {
            FilterOperator.EQUAL: "$eq",
            FilterOperator.NOT_EQUAL: "$ne",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$lte",
            FilterOperator.IN: "$in",
            FilterOperator.NOT_IN: "$nin",
        }
        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by Pinecone!")
        return mapping[operator]

    def _translate_metadata_condition(self, conditions: List[FilterCondition]) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by pinecone.
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
        pinecone_conditions = []
        for condition in metadata_conditions:
            metadata_key = condition.column.split(".")[-1]
            pinecone_conditions.append(
                {
                    metadata_key: {
                        self._get_pinecone_operator(condition.op): condition.value
                    }
                }
            )

        # we combine all metadata conditions into a single dict
        metadata_condition = (
            {"$and": pinecone_conditions}
            if len(pinecone_conditions) > 1
            else pinecone_conditions[0]
        )
        return metadata_condition

    def connect(self):
        """Connect to a pinecone database."""
        try:
            pinecone.init(api_key=self._client_config["api_key"], environment=self._client_config["environment"])
            pinecone.list_indexes()
            self.is_connected = True
        except Exception as e:
            log.logger.error(f"Error connecting to Pinecone client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the pinecone connection."""
        if self.is_connected is False:
            return
        pinecone.init(api_key="", environment="")
        self.is_connected = False

    def check_connection(self):
        """Check the connection to pinecone."""
        response_code = StatusResponse(False)
        try:
            pinecone.list_indexes()
            response_code.success = True
        except Exception as e:
            log.logger.error(f"Error connecting to pinecone , {e}!")
            response_code.error_message = str(e)
        return response_code

    def get_tables(self) -> HandlerResponse:
        """Get the list of indexes in the pinecone database."""
        indexes = pinecone.list_indexes()
        indexes_names = pd.DataFrame(
            columns=["index_name"],
            data=[index for index in indexes],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=indexes_names)

    def insert(self, table_name: str, data: pd.DataFrame, columns: List[str] = None) -> HandlerResponse:
        """Insert data into pinecone index passed in through `table_name` parameter."""
        upsert_size = 99
        index = self._get_index_handle(table_name)
        if index is None:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting index '{table_name}', are you sure the name is correct?"
            )
        data.dropna(axis=1, inplace=True)
        data.rename(columns={
            TableField.ID.value: "id",
            TableField.EMBEDDINGS.value: "values",
            TableField.METADATA.value: "metadata",
            TableField.CONTENT.value: "sparse_values"},
            inplace=True)
        data = data[["id", "values", "metadata", "sparse_values"]]
        try:
            for chunk in (data[pos:pos + upsert_size] for pos in range(0, len(data), upsert_size)):
                chunk = chunk.to_dict(orient="records")
                index.upsert(vectors=chunk)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error upserting data into {table_name}: {e}"
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """Delete an index passed in through `table_name` from the pinecone ."""
        try:
            pinecone.delete_index(table_name)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error deleting index {table_name}: {e}"
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def _get_pinecone_operator(self, operator: FilterOperator) -> str:
        """Convert FilterOperator to an operator that pinecone's query language can undersand"""
        mapping = {
            FilterOperator.EQUAL: "$eq",
            FilterOperator.NOT_EQUAL: "$ne",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$lte",
            FilterOperator.IN: "$in",
            FilterOperator.NOT_IN: "$nin",
        }
        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by Pinecone!")
        return mapping[operator]

    def _translate_metadata_condition(self, conditions: List[FilterCondition]) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by pinecone.
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
        pinecone_conditions = []
        for condition in metadata_conditions:
            metadata_key = condition.column.split(".")[-1]
            pinecone_conditions.append(
                {
                    metadata_key: {
                        self._get_pinecone_operator(condition.op): condition.value
                    }
                }
            )

        # we combine all metadata conditions into a single dict
        metadata_condition = (
            {"$and": pinecone_conditions}
            if len(pinecone_conditions) > 1
            else pinecone_conditions[0]
        )
        return metadata_condition

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> HandlerResponse:
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

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Update data in the ChromaDB database.
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
