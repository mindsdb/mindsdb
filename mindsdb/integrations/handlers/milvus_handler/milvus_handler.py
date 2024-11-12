from typing import List, Optional

import pandas as pd
import json
from pymilvus import MilvusClient, CollectionSchema, DataType, FieldSchema

from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator, TableField, VectorStoreHandler
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MilvusHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Milvus statements."""

    name = "milvus"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.milvus_client = None
        self._connection_data = kwargs["connection_data"]
        # Extract parameters used while searching and leave the rest for establishing connection
        self._search_limit = 100
        if "search_default_limit" in self._connection_data:
            self._search_limit = self._connection_data["search_default_limit"]
        self._search_params = {
            "search_metric_type": "L2",
            "search_ignore_growing": False,
            "search_params": {"nprobe": 10},
        }
        for search_param_name in self._search_params:
            if search_param_name in self._connection_data:
                self._search_params[search_param_name] = self._connection_data[search_param_name]
        # Extract parameters used for creating tables
        self._create_table_params = {
            "create_auto_id": False,
            "create_id_max_len": 64,
            "create_embedding_dim": 8,
            "create_dynamic_field": True,
            "create_content_max_len": 200,
            "create_content_default_value": "",
            "create_schema_description": "MindsDB generated table",
            "create_alias": "default",
            "create_index_params": {},
            "create_index_metric_type": "L2",
            "create_index_type": "AUTOINDEX",
        }
        for create_table_param in self._create_table_params:
            if create_table_param in self._connection_data:
                self._create_table_params[create_table_param] = self._connection_data[create_table_param]
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
            self.milvus_client = MilvusClient(**self._connection_data)
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error connecting to Milvus client: {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected is False:
            return
        self.milvus_client.close()
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Milvus database."""
        response_code = StatusResponse(False)
        try:
            response_code.success = self.milvus_client is not None
        except Exception as e:
            logger.error(f"Error checking Milvus connection: {e}!")
            response_code.error_message = str(e)
        return response_code

    def get_tables(self) -> HandlerResponse:
        """Get the list of collections in the Milvus database."""
        collections = self.milvus_client.list_collections()
        collections_name = pd.DataFrame(
            columns=["TABLE_NAME"],
            data=[collection for collection in collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def drop_table(self, table_name: str, if_exists=True):
        """Delete a collection from the Milvus database."""
        try:
            self.milvus_client.drop_collection(collection_name=table_name)
        except Exception as e:
            if not if_exists:
                raise Exception(f"Error dropping table '{table_name}': {e}")

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

    def _translate_conditions(self, conditions: Optional[List[FilterCondition]], exclude_id: bool = True) -> Optional[str]:
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
        If exclude_id is set to true then id column is ignored
        """
        if not conditions:
            return
        # Ignore all non-metadata conditions
        filtered_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value) or condition.column.startswith(TableField.ID.value)
        ]
        if len(filtered_conditions) == 0:
            return None
        # Translate each metadata condition into a dict
        milvus_conditions = []
        for condition in filtered_conditions:
            if isinstance(condition.value, str):
                condition.value = f"'{condition.value}'"
            milvus_conditions.append(f"({condition.column.split('.')[-1]} {self._get_milvus_operator(condition.op)} {condition.value})")
        # Combine all metadata conditions into a single string and return
        return " and ".join(milvus_conditions) if milvus_conditions else None

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ):
        self.milvus_client.load_collection(collection_name=table_name)
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

        # Generate search arguments
        search_arguments = {}
        # TODO: check if distance in columns work
        if columns:
            search_arguments["output_fields"] = columns
        else:
            search_arguments["output_fields"] = [schema_obj.name for schema_obj in self.SCHEMA]
        search_arguments["filter"] = self._translate_conditions(conditions)
        # NOTE: According to api sum of offset and limit should be less than 16384.
        api_limit = 16384
        if limit is not None and offset is not None and limit + offset >= api_limit:
            raise Exception(f"Sum of limit and offset should be less than {api_limit}")

        if limit is not None:
            search_arguments["limit"] = limit
        else:
            search_arguments["limit"] = self._search_limit
        if offset is not None:
            search_arguments["offset"] = offset

        # Vector search
        if vector_filter:
            search_arguments["data"] = vector_filter
            search_arguments["anns_field"] = TableField.EMBEDDINGS.value
            if "search_params" not in search_arguments:
                search_arguments["search_params"] = {}
            search_arguments["search_params"]["metric_type"] = self._search_params["search_metric_type"]
            search_arguments["search_params"]["ignore_growing"] = self._search_params["search_ignore_growing"]
            results = self.milvus_client.search(table_name, **search_arguments)[0]
            columns_required = [TableField.ID.value, TableField.DISTANCE.value]
            if TableField.CONTENT.value in columns:
                columns_required.append(TableField.CONTENT.value)
            if TableField.EMBEDDINGS.value in columns:
                columns_required.append(TableField.EMBEDDINGS.value)
            data = {k: [] for k in columns_required}
            for hit in results:
                for col in columns_required:
                    if col != TableField.DISTANCE.value:
                        data[col].append(hit["entity"].get(col))
                    else:
                        data[TableField.DISTANCE.value].append(hit["distance"])
            return pd.DataFrame(data)
        else:
            # Basic search
            if not search_arguments["filter"]:
                search_arguments["filter"] = ""
            search_arguments["output_fields"] = [
                TableField.ID.value,
                TableField.CONTENT.value,
                TableField.EMBEDDINGS.value,
            ] if not columns else columns
            results = self.milvus_client.query(table_name, **search_arguments)
            return pd.DataFrame.from_records(results)

    def create_table(self, table_name: str, if_not_exists=True):
        """Create a collection with default parameters in the Milvus database as described in documentation."""
        id = FieldSchema(
            name=TableField.ID.value,
            dtype=DataType.VARCHAR,
            is_primary=True,
            max_length=self._create_table_params["create_id_max_len"],
            auto_id=self._create_table_params["create_auto_id"]
        )
        embeddings = FieldSchema(
            name=TableField.EMBEDDINGS.value,
            dtype=DataType.FLOAT_VECTOR,
            dim=self._create_table_params["create_embedding_dim"]
        )
        content = FieldSchema(
            name=TableField.CONTENT.value,
            dtype=DataType.VARCHAR,
            max_length=self._create_table_params["create_content_max_len"],
            default_value=self._create_table_params["create_content_default_value"]
        )
        schema = CollectionSchema(
            fields=[id, content, embeddings],
            description=self._create_table_params["create_schema_description"],
            enable_dynamic_field=self._create_table_params["create_dynamic_field"]
        )
        collection_name = table_name
        self.milvus_client.create_collection(
            collection_name=collection_name,
            schema=schema
        )
        index_params = self.milvus_client.prepare_index_params()
        index_params.add_index(
            field_name=TableField.EMBEDDINGS.value,
            index_type=self._create_table_params["create_index_type"],
            metric_type=self._create_table_params["create_index_metric_type"],
            params=self._create_table_params.get("create_params", {})
        )
        self.milvus_client.create_index(
            collection_name=collection_name,
            index_params=index_params,
        )

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """Insert data into the Milvus collection."""
        self.milvus_client.load_collection(collection_name=table_name)
        if columns:
            data = data[columns]
        if TableField.METADATA.value in data.columns:
            rows = data[TableField.METADATA.value].to_list()
            for i, row in enumerate(rows):
                if isinstance(row, str):
                    rows[i] = json.loads(row)
            data = pd.concat([data, pd.DataFrame.from_records(rows)], axis=1)
            data.drop(TableField.METADATA.value, axis=1, inplace=True)
        data_list = data.to_dict(orient="records")
        for data_dict in data_list:
            if TableField.EMBEDDINGS.value in data_dict and isinstance(data_dict[TableField.EMBEDDINGS.value], str):
                data_dict[TableField.EMBEDDINGS.value] = json.loads(data_dict[TableField.EMBEDDINGS.value])
        self.milvus_client.insert(table_name, data_list)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        # delete only supports IN operator
        for condition in conditions:
            if condition.op in [FilterOperator.EQUAL, FilterOperator.IN]:
                condition.op = FilterOperator.IN
                if not isinstance(condition.value, list):
                    condition.value = [condition.value]
        filters = self._translate_conditions(conditions, exclude_id=False)
        if not filters:
            raise Exception("Some filters are required, use DROP TABLE to delete everything")
        if self.milvus_client.has_collection(collection_name=table_name):
            self.milvus_client.delete(table_name, filter=filters)

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get columns in a Milvus collection"""
        try:
            self.milvus_client.has_collection(collection_name=table_name)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error finding table: {e}",
            )
        try:
            field_names = {field["name"] for field in self.milvus_client.describe_collection(collection_name=table_name)["fields"]}
            schema = [mindsdb_schema_field for mindsdb_schema_field in self.SCHEMA if mindsdb_schema_field["name"] in field_names]
            data = pd.DataFrame(schema)
            data.columns = ["COLUMN_NAME", "DATA_TYPE"]
            return HandlerResponse(data_frame=data)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error finding table: {e}",
            )
