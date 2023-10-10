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
            "search_params": "param"
        }
        self._search_params = {}
        for search_param_alias, actual_search_param_name in search_param_names.items():
            if search_param_alias in self._connection_data:
                self._search_params[actual_search_param_name] = self._connection_data[search_param_alias]
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

    def _get_milvus_operator(self, operator: FilterOperator) -> str:
        mapping = {
            FilterOperator.EQUAL: "==",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">=",
            FilterOperator.IN: "in",
            FilterOperator.NOT_IN: "not in",
            FilterOperator.LIKE: "like",
            FilterOperator.NOT_LIKE: "not like",
        }
        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by Milvus!")
        return mapping[operator]

    def _translate_metadata_conditions(
        self, conditions: List[FilterCondition]
    ) -> Optional[str]:
        """
        Translate a list of FilterCondition objects a string that can be used by Milvus.
        E.g.,
        [
            FilterCondition(
                column="metadata.price",
                op=FilterOperator.LESS_THAN,
                value=1000,
            ),
            FilterCondition(
                column="metadata.price",
                op=FilterOperator.GREATER_THAN,
                value=300,
            )
        ]
        Is converted to: "(price < 1000) and (price > 300)"
        """
        # Ignore all non-metadata conditions
        if conditions is None:
            return None
        metadata_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        if len(metadata_conditions) == 0:
            return None
        # Translate each metadata condition into a dict
        milvus_conditions = []
        for condition in metadata_conditions:
            milvus_conditions.append(
                f"({condition.column.split('.')[-1]} {self._get_chromadb_operator(condition.op)} {condition.value})")
        # Combine all metadata conditions into a single string and return
        return " and ".join(milvus_conditions) if milvus_conditions else None

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
                condition.value
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )

        # Generate search parameters
        search_arguments = {}
        # TODO: check if distance in columns work
        if columns:
            search_arguments["output_fields"] = columns
        else:
            search_arguments["output_fields"] = [
                schema_obj.name for schema_obj in self.SCHEMA]
        search_arguments["expr"] = self._translate_metadata_conditions(
            conditions)
        # NOTE: According to api sum of offset and limit should be less than 16384.
        api_limit = 16384
        if limit is not None and offset is not None and limit + offset >= api_limit:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Sum of limit and offset should be less than {api_limit}",
            )
        if limit is not None:
            search_arguments["limit"] = limit
        if offset is not None:
            search_arguments["param"]["offset"] = offset

        # Execute query
        results = None
        if vector_filter:
            # Vector search
            search_arguments["data"] = vector_filter
            search_arguments["anns_field"] = TableField.EMBEDDINGS.value
            search_arguments["param"] = self._search_params
            results = collection.search(**search_arguments)
            columns_required = [
                TableField.ID.value,
                TableField.DISTANCE.value
            ]
            if TableField.CONTENT.value in columns:
                columns_required.append(TableField.CONTENT.value)
            if TableField.EMBEDDINGS.value in columns:
                columns_required.append(TableField.EMBEDDINGS.value)
            # TODO: convert metadata somehow
            # if TableField.METADATA.value in columns:
            #    columns_required.append(TableField.METADATA.value)
            data = {k: [] for k in columns_required}
            for hits in results:
                for hit in hits:
                    for col in columns_required:
                        if col != TableField.DISTANCE.value:
                            data[col].append(hit.entity.get(col))
                        else:
                            data[TableField.DISTANCE.value].append(hit.distance)
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))
        else:
            # Basic search
            if not search_arguments["expr"]:
                search_arguments["expr"] = ""
                # If no expression, query requires a limit
                if "limit" not in search_arguments:
                    search_arguments["limit"] = 100
            search_arguments["output_fields"] = [
                TableField.ID.value,
                TableField.CONTENT.value,
                TableField.EMBEDDINGS.value,
                TableField.METADATA.value,
            ] if not columns else columns
            results = collection.query(**search_arguments)
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame.from_records(results))

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
    alias={
        "type": ARG_TYPE.STR,
        "description": "alias of the Milvus connection to construct",
        "required": True,
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "IP address of the Milvus server",
        "required": True,
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "port of the Milvus server",
        "required": True,
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "username of the Milvus server",
        "required": True,
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "password of the username of the Milvus server",
        "required": True,
    },
    search_metric_type={
        "type": ARG_TYPE.STR,
        "description": "metric type used for searches",
        "required": False,
    },
    search_ignore_growing={
        "type": ARG_TYPE.BOOL,
        "description": "whether to ignore growing segments during similarity searches",
        "required": False,
    },
    search_params={
        "type": ARG_TYPE.DICT,
        "description": "specific to the `search_metric_type`",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    alias="default",
    host="127.0.0.1",
    port=19530,
    user="username",
    password="password",
    search_metric_type="L2",
    search_ignore_growing=True,
    search_params={"nprobe": 10},
)
