import ast
from typing import List, Optional

import numpy as np
from pinecone import Pinecone, ServerlessSpec
from pinecone.core.openapi.shared.exceptions import NotFoundException, PineconeApiException
import pandas as pd

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

logger = log.getLogger(__name__)

DEFAULT_CREATE_TABLE_PARAMS = {
    "dimension": 8,
    "metric": "cosine",
    "spec": {
        "cloud": "aws",
        "region": "us-east-1"
    }
}
MAX_FETCH_LIMIT = 10000
UPSERT_BATCH_SIZE = 99  # API reccomendation


class PineconeHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Pinecone statements."""

    name = "pinecone"

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _get_index_handle(self, index_name):
        """Returns handler to index specified by `index_name`"""
        connection = self.connect()
        index = connection.Index(index_name)
        try:
            index.describe_index_stats()
        except Exception:
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

    def _matches_to_dicts(self, matches: List):
        """Converts the custom pinecone response type to a list of python dict"""
        return [match.to_dict() for match in matches]

    def connect(self):
        """Connect to a pinecone database."""
        if self.is_connected is True:
            return self.connection

        if 'api_key' not in self.connection_data:
            raise ValueError('Required parameter (api_key) must be provided.')

        try:
            self.connection = Pinecone(api_key=self.connection_data['api_key'])
            return self.connection
        except Exception as e:
            logger.error(f"Error connecting to Pinecone client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the pinecone connection."""
        if self.is_connected is False:
            return
        self.connection = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to pinecone."""
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.list_indexes()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to pinecone , {e}!")
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def get_tables(self) -> HandlerResponse:
        """Get the list of indexes in the pinecone database."""
        connection = self.connect()
        indexes = connection.list_indexes()
        df = pd.DataFrame(
            columns=["table_name"],
            data=[index['name'] for index in indexes],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)

    def create_table(self, table_name: str, if_not_exists=True):
        """Create an index with the given name in the Pinecone database."""
        connection = self.connect()

        # TODO: Should other parameters be supported? Pod indexes?
        # TODO: Should there be a better way to provide these parameters rather than when establishing the connection?
        create_table_params = {}
        for key, val in DEFAULT_CREATE_TABLE_PARAMS.items():
            if key in self.connection_data:
                create_table_params[key] = self.connection_data[key]
            else:
                create_table_params[key] = val

        create_table_params["spec"] = ServerlessSpec(**create_table_params["spec"])

        try:
            connection.create_index(name=table_name, **create_table_params)
        except PineconeApiException as pinecone_error:
            if pinecone_error.status == 409 and if_not_exists:
                return
            raise Exception(f"Error creating index '{table_name}': {pinecone_error}")

    def insert(self, table_name: str, data: pd.DataFrame):
        """Insert data into pinecone index passed in through `table_name` parameter."""
        index = self._get_index_handle(table_name)
        if index is None:
            raise Exception(f"Error getting index '{table_name}', are you sure the name is correct?")

        data.rename(columns={
            TableField.ID.value: "id",
            TableField.EMBEDDINGS.value: "values"},
            inplace=True)

        columns = ["id", "values"]

        if TableField.METADATA.value in data.columns:
            data.rename(columns={TableField.METADATA.value: "metadata"}, inplace=True)
            # fill None and NaN values with empty dict
            if data['metadata'].isnull().any():
                data['metadata'] = data['metadata'].apply(lambda x: {} if x is None or (isinstance(x, float) and np.isnan(x)) else x)
            columns.append("metadata")

        data = data[columns]

        # convert the embeddings to lists if they are strings
        data["values"] = data["values"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        for chunk in (data[pos:pos + UPSERT_BATCH_SIZE] for pos in range(0, len(data), UPSERT_BATCH_SIZE)):
            chunk = chunk.to_dict(orient="records")
            index.upsert(vectors=chunk)

    def drop_table(self, table_name: str, if_exists=True):
        """Delete an index passed in through `table_name` from the pinecone ."""
        connection = self.connect()
        try:
            connection.delete_index(table_name)
        except NotFoundException:
            if if_exists:
                return
            raise Exception(f"Error deleting index '{table_name}', are you sure the name is correct?")

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        """Delete records in pinecone index `table_name` based on ids or based on metadata conditions."""
        filters = self._translate_metadata_condition(conditions)
        ids = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ] or None
        if filters is None and ids is None:
            raise Exception("Delete query must have either id condition or metadata condition!")
        index = self._get_index_handle(table_name)
        if index is None:
            raise Exception(f"Error getting index '{table_name}', are you sure the name is correct?")

        if filters is None:
            index.delete(ids=ids)
        else:
            index.delete(filter=filters)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ):
        """Run query on pinecone index named `table_name` and get results."""
        # TODO: Add support for namespaces.
        index = self._get_index_handle(table_name)
        if index is None:
            raise Exception(f"Error getting index '{table_name}', are you sure the name is correct?")

        query = {
            "include_values": True,
            "include_metadata": True
        }

        # check for metadata filter
        metadata_filters = self._translate_metadata_condition(conditions)
        if metadata_filters is not None:
            query["filter"] = metadata_filters

        # check for vector and id filters
        vector_filters = []
        id_filters = []

        if conditions:
            for condition in conditions:
                if condition.column == TableField.SEARCH_VECTOR.value:
                    vector_filters.append(condition.value)
                elif condition.column == TableField.ID.value:
                    id_filters.append(condition.value)

        if vector_filters:
            if len(vector_filters) > 1:
                raise Exception("You cannot have multiple search_vectors in query")

            query["vector"] = vector_filters[0]
            # For subqueries, the vector filter is a list of list of strings
            if isinstance(query["vector"], list) and isinstance(query["vector"][0], str):
                if len(query["vector"]) > 1:
                    raise Exception("You cannot have multiple search_vectors in query")

                try:
                    query["vector"] = ast.literal_eval(query["vector"][0])
                except Exception as e:
                    raise Exception(f"Cannot parse the search vector '{query['vector']}'into a list: {e}")

        if id_filters:
            if len(id_filters) > 1:
                raise Exception("You cannot have multiple IDs in query")

            query["id"] = id_filters[0]

        if not vector_filters and not id_filters:
            raise Exception("You must provide either a search_vector or an ID in the query")

        # check for limit
        if limit is not None:
            query["top_k"] = limit
        else:
            query["top_k"] = MAX_FETCH_LIMIT

        # exec query
        try:
            result = index.query(**query)
        except Exception as e:
            raise Exception(f"Error running SELECT query on '{table_name}': {e}")

        # convert to dataframe
        df_columns = {
            "id": TableField.ID.value,
            "metadata": TableField.METADATA.value,
            "values": TableField.EMBEDDINGS.value,
        }
        results_df = pd.DataFrame.from_records(self._matches_to_dicts(result["matches"]))
        if bool(len(results_df.columns)):
            results_df.rename(columns=df_columns, inplace=True)
        else:
            results_df = pd.DataFrame(columns=list(df_columns.values()))
        results_df[TableField.CONTENT.value] = ""
        return results_df[columns]

    def get_columns(self, table_name: str) -> HandlerResponse:
        return super().get_columns(table_name)
