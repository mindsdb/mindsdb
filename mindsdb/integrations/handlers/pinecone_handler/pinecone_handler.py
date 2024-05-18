from typing import List, Optional

import pinecone
import pandas as pd
import ast

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


class PineconeHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Pinecone statements."""

    name = "pinecone"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.MAX_FETCH_LIMIT = 10000
        self._connection_data = kwargs.get("connection_data")
        self._client_config = {
            "api_key": self._connection_data.get("api_key"),
            "environment": self._connection_data.get("environment")
        }
        self._table_create_params = {
            "dimension": 8,
            "metric": "cosine",
            "pods": 1,
            "replicas": 1,
            "pod_type": 'p1',
        }
        for key in self._table_create_params:
            if key in self._connection_data:
                self._table_create_params[key] = self._connection_data[key]
        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _get_index_handle(self, index_name):
        """Returns handler to index specified by `index_name`"""
        index = pinecone.Index(index_name)
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
        try:
            pinecone.init(api_key=self._client_config["api_key"], environment=self._client_config["environment"])
            pinecone.list_indexes()
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error connecting to Pinecone client, {e}!")
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
            logger.error(f"Error connecting to pinecone , {e}!")
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

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """Create an index with the given name in the Pinecone database."""
        try:
            pinecone.create_index(name=table_name, **self._table_create_params)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error creating index '{table_name}': {e}"
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def insert(self, table_name: str, data: pd.DataFrame, columns: List[str] = None) -> HandlerResponse:
        """Insert data into pinecone index passed in through `table_name` parameter."""
        upsert_batch_size = 99  # API reccomendation
        index = self._get_index_handle(table_name)
        if index is None:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting index '{table_name}', are you sure the name is correct?"
            )
        data.rename(columns={
            TableField.ID.value: "id",
            TableField.EMBEDDINGS.value: "values",
            TableField.METADATA.value: "metadata"},
            inplace=True)
        data = data[["id", "values", "metadata"]]
        try:
            for chunk in (data[pos:pos + upsert_batch_size] for pos in range(0, len(data), upsert_batch_size)):
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

    def delete(self, table_name: str, conditions: List[FilterCondition] = None) -> HandlerResponse:
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
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting index '{table_name}', are you sure the name is correct?"
            )
        try:
            if filters is None:
                index.delete(ids=ids)
            else:
                index.delete(filter=filters)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error deleting records in '{table_name}': {e}"
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
        """Run query on pinecone index named `table_name` and get results."""
        index = self._get_index_handle(table_name)
        if index is None:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting index '{table_name}', are you sure the name is correct?"
            )
        query = {
            "include_values": True,
            "include_metadata": True
        }
        # check for metadata filter
        metadata_filters = self._translate_metadata_condition(conditions)
        # check for vector filter
        vector_filter = (
            None
            if conditions is None
            else [
                condition.value
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )
        if vector_filter:
            if len(vector_filter) > 1:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message="You cannot have multiple search_vectors in query"
                )
            query["vector"] = vector_filter[0]
            # For subqueries, the vector filter is a list of list of strings
            if isinstance(query["vector"], list) and isinstance(query["vector"][0], str):
                if len(query["vector"]) > 1:
                    return Response(
                        resp_type=RESPONSE_TYPE.ERROR,
                        error_message="You cannot have multiple search_vectors in query"
                    )
                try:
                    query["vector"] = ast.literal_eval(query["vector"][0])
                except Exception as e:
                    return Response(
                        resp_type=RESPONSE_TYPE.ERROR,
                        error_message=f"Cannot parse the search vector '{query['vector']}'into a list: {e}"
                    )
        # check for limit
        if limit is not None:
            query["top_k"] = limit
        else:
            query["top_k"] = self.MAX_FETCH_LIMIT
        if metadata_filters is not None:
            query["filter"] = metadata_filters
        # check for id filter
        id_filters = None
        if conditions is not None:
            id_filters = [
                condition.value
                for condition in conditions
                if condition.column == TableField.ID.value
            ] or None
        if id_filters:
            if len(id_filters) > 1:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message="You cannot have multiple IDs in query"
                )
            query["id"] = id_filters[0]
        # exec query
        result = None
        try:
            result = index.query(**query)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error running SELECT query on '{table_name}': {e}"
            )
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
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=results_df[columns])

    def get_columns(self, table_name: str) -> HandlerResponse:
        return super().get_columns(table_name)
