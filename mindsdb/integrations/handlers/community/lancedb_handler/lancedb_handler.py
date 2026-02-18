from typing import List, Optional

import lancedb
import pandas as pd
import pyarrow as pa
from lance.vector import vec_to_table
import duckdb
import json

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


class LanceDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the LanceDB statements."""

    name = "lancedb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs.get("connection_data")

        self._client_config = {
            "uri": self._connection_data.get("persist_directory"),
            "api_key": self._connection_data.get("api_key", None),
            "region": self._connection_data.get("region"),
            "host_override": self._connection_data.get("host_override"),
        }

        # uri is required either for LanceDB Cloud or local
        if not self._client_config["uri"]:
            raise Exception(
                "persist_directory is required for LanceDB connection!"
            )
        # uri, api_key and region is required either for LanceDB Cloud
        elif self._client_config["uri"] and self._client_config["api_key"] and not self._client_config["region"]:
            raise Exception(
                "region is required for LanceDB Cloud connection!"
            )

        self._client = None
        self.is_connected = False
        self.connect()

    def _get_client(self):
        client_config = self._client_config
        if client_config is None:
            raise Exception("Client config is not set!")
        return lancedb.connect(**client_config)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """Connect to a LanceDB database."""
        if self.is_connected is True:
            return
        try:
            self._client = self._get_client()
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error connecting to LanceDB client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected is False:
            return
        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the LanceDB database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False
        try:
            self._client.table_names()
            response_code.success = True
        except Exception as e:
            logger.error(f"Error connecting to LanceDB , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False

        return response_code

    def _get_lancedb_operator(self, operator: FilterOperator) -> str:
        # The in values are not returned with () and only one element is returned. Bug
        mapping = {
            FilterOperator.EQUAL: "=",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">=",
            FilterOperator.IN: "in",
            FilterOperator.NOT_IN: "not in",
            FilterOperator.LIKE: "like",
            FilterOperator.NOT_LIKE: "not like",
            FilterOperator.IS_NULL: "is null",
            FilterOperator.IS_NOT_NULL: "is not null",
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by LanceDB!")

        return mapping[operator]

    def _translate_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects to string that can be used by LanceDB.
        E.g.,
        [
            FilterCondition(
                column="content",
                op=FilterOperator.NOT_EQUAL,
                value="a",
            ),
            FilterCondition(
                column="id",
                op=FilterOperator.EQUAL,
                value="6",
            )
        ]
        -->
            "content != 'a' and id = '6'"
        """
        # we ignore all non-metadata conditions
        if not conditions:
            return
        filtered_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.ID.value) or condition.column.startswith(TableField.CONTENT.value)
        ]

        if len(filtered_conditions) == 0:
            return None

        # generate the LanceDB filter string
        lancedb_conditions = []
        for condition in filtered_conditions:
            if isinstance(condition.value, str):
                condition.value = f"'{condition.value}'"
            condition_key = condition.column.split(".")[-1]

            value = condition.value
            if condition.op in (FilterOperator.IN, FilterOperator.NOT_IN):
                if not isinstance(condition.value, list):
                    value = [value]
                value = '({})'.format(', '.join([repr(i) for i in value]))
            else:
                value = str(value)
            lancedb_conditions.append(
                ' '.join([condition_key, self._get_lancedb_operator(condition.op), value])
            )
        # Combine all conditions into a single string and return
        return " and ".join(lancedb_conditions) if lancedb_conditions else None

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:

        collection = self._client.open_table(table_name)

        filters = self._translate_condition(conditions)
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

        if vector_filter is not None:
            vec = json.loads(vector_filter.value) if isinstance(vector_filter.value, str) else vector_filter.value
            result = collection.search(vec).select(columns).to_pandas()
            result = result.rename(columns={"_distance": TableField.DISTANCE.value})
        else:
            result = self._client.open_table(table_name).to_pandas()

        new_columns = columns + [TableField.DISTANCE.value] if TableField.DISTANCE.value in result.columns else columns

        col_str = ', '.join([col for col in new_columns if col in (TableField.ID.value, TableField.CONTENT.value, TableField.METADATA.value, TableField.EMBEDDINGS.value, TableField.DISTANCE.value)])

        where_str = f'where {filters}' if filters else ''
        # implementing limit and offset. Not supported natively in lancedb
        if limit and offset:
            sql = f"""select {col_str} from result {where_str} limit {limit} offset {offset}"""
        elif limit and not offset:
            sql = f"""select {col_str} from result {where_str} limit {limit}"""
        elif offset and not limit:
            sql = f"""select {col_str} from result {where_str} offset {offset}"""
        else:
            sql = f"""select {col_str} from result {where_str}"""

        data_df = duckdb.query(sql).to_df()
        return data_df

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """
        Insert data into the LanceDB database.
        In case of create table statements the there is a mismatch between the column types of the `data` pandas dataframe filled with data
        and the empty base table column types which raises a pa.lib.ArrowNotImplementedError, in that case the base table is deleted (doesn't matter as it is empty)
        and recreated with the right datatypes
        """

        if TableField.METADATA.value not in data.columns:
            data[TableField.METADATA.value] = None

        df = data[
            [TableField.ID.value, TableField.CONTENT.value, TableField.METADATA.value, TableField.EMBEDDINGS.value]
        ]

        try:
            collection = self._client.open_table(table_name)
            pa_data = pa.Table.from_pandas(df, preserve_index=False)
            vec_data = vec_to_table(df[TableField.EMBEDDINGS.value].values.tolist())
            new_pa_data = pa_data.append_column("vector", vec_data["vector"])
            collection.add(new_pa_data)
        except pa.lib.ArrowNotImplementedError:
            collection_df = collection.to_pandas()
            column_dtypes = collection_df.dtypes
            df = df.astype(column_dtypes)
            new_df = pd.concat([collection_df, df])
            new_df['id'] = new_df['id'].apply(str)
            pa_data = pa.Table.from_pandas(new_df, preserve_index=False)
            vec_data = vec_to_table(df[TableField.EMBEDDINGS.value].values.tolist())
            new_pa_data = pa_data.append_column("vector", vec_data["vector"])
            self.drop_table(table_name)
            self._client.create_table(table_name, new_pa_data)

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """
        Update data in the LanceDB database.
        TODO: not implemented yet
        """
        return super().update(table_name, data, columns)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        filters = self._translate_condition(conditions)
        if filters is None:
            raise Exception("Delete query must have at least one condition!")
        collection = self._client.open_table(table_name)
        collection.delete(filters)

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Create a collection with the given name in the LanceDB database.
        """

        data = {
            TableField.ID.value: str,
            TableField.CONTENT.value: str,
            TableField.METADATA.value: object,
            TableField.EMBEDDINGS.value: object,
        }
        df = pd.DataFrame(columns=data.keys()).astype(data)
        self._client.create_table(table_name, df)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Delete a collection from the LanceDB database.
        """
        try:
            self._client.drop_table(table_name)
        except ValueError as e:
            if not if_exists:
                raise e

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of collections in the LanceDB database.
        """
        collections = self._client.table_names()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=collections,
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        # check if collection exists
        try:
            df = self._client.open_table(table_name).to_pandas()
            column_df = pd.DataFrame(df.dtypes).reset_index()
            column_df.columns = ['column_name', 'data_type']
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=column_df)
