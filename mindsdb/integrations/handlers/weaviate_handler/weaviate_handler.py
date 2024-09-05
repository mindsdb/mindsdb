import ast
from datetime import datetime
from typing import List, Optional

import weaviate
from weaviate.embedded import EmbeddedOptions
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
from weaviate.util import generate_uuid5

logger = log.getLogger(__name__)


class WeaviateDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Weaviate statements."""

    name = "weaviate"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)

        self._connection_data = kwargs.get("connection_data")

        self._client_config = {
            "weaviate_url": self._connection_data.get("weaviate_url"),
            "weaviate_api_key": self._connection_data.get("weaviate_api_key"),
            "persistence_directory": self._connection_data.get("persistence_directory"),
        }

        if not (
            self._client_config.get("weaviate_url")
            or self._client_config.get("persistence_directory")
        ):
            raise Exception(
                "Either url or persist_directory is required for weaviate connection!"
            )

        self._client = None
        self._embedded_options = None
        self.is_connected = False
        self.connect()

    def _get_client(self) -> weaviate.Client:
        if not (
            self._client_config
            and (
                self._client_config.get("weaviate_url")
                or self._client_config.get("persistence_directory")
            )
        ):
            raise Exception("Client config is not set! or missing parameters")

        # decide the client type to be used, either persistent or httpclient
        if self._client_config.get("persistence_directory"):
            self._embedded_options = EmbeddedOptions(
                persistence_data_path=self._client_config.get("persistence_directory")
            )
            return weaviate.Client(embedded_options=self._embedded_options)
        if self._client_config.get("weaviate_api_key"):
            return weaviate.Client(
                url=self._client_config["weaviate_url"],
                auth_client_secret=weaviate.AuthApiKey(
                    api_key=self._client_config["weaviate_api_key"]
                ),
            )
        return weaviate.Client(url=self._client_config["weaviate_url"])

    def __del__(self):
        self.is_connected = False
        if self._embedded_options:
            self._client._connection.embedded_db.stop()
            del self._embedded_options
        self._embedded_options = None
        self._client._connection.close()
        if self._client:
            del self._client

    def connect(self):
        """Connect to a weaviate database."""
        if self.is_connected:
            return self._client

        try:
            self._client = self._get_client()
            self.is_connected = True
            return self._client
        except Exception as e:
            logger.error(f"Error connecting to weaviate client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""

        if not self.is_connected:
            return
        if self._embedded_options:
            self._client._connection.embedded_db.stop()
            del self._embedded_options
        del self._client
        self._embedded_options = None
        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Weaviate database."""
        response_code = StatusResponse(False)

        try:
            if self._client.is_live():
                response_code.success = True
        except Exception as e:
            logger.error(f"Error connecting to weaviate , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success and not self.is_connected:
                self.disconnect()
            if not response_code.success and self.is_connected:
                self.is_connected = False

        return response_code

    @staticmethod
    def _get_weaviate_operator(operator: FilterOperator) -> str:
        mapping = {
            FilterOperator.EQUAL: "Equal",
            FilterOperator.NOT_EQUAL: "NotEqual",
            FilterOperator.LESS_THAN: "LessThan",
            FilterOperator.LESS_THAN_OR_EQUAL: "LessThanEqual",
            FilterOperator.GREATER_THAN: "GreaterThan",
            FilterOperator.GREATER_THAN_OR_EQUAL: "GreaterThanEqual",
            FilterOperator.IS_NULL: "IsNull",
            FilterOperator.LIKE: "Like",
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by weaviate!")

        return mapping[operator]

    @staticmethod
    def _get_weaviate_value_type(value) -> str:
        # https://github.com/weaviate/weaviate-python-client/blob/c760b1d59b2a222e770d53cc257b1bf993a0a592/weaviate/gql/filter.py#L18
        if isinstance(value, list):
            value_list_types = {
                str: "valueTextList",
                int: "valueIntList",
                float: "valueIntList",
                bool: "valueBooleanList",
            }
            if not value:
                raise Exception("Empty list is not supported")
            value_type = value_list_types.get(type(value[0]))

        else:
            value_primitive_types = {
                str: "valueText",
                int: "valueInt",
                float: "valueInt",
                datetime: "valueDate",
                bool: "valueBoolean",
            }
            value_type = value_primitive_types.get(type(value))

        if not value_type:
            raise Exception(f"Value type {type(value)} is not supported by weaviate!")

        return value_type

    def _translate_condition(
        self,
        table_name: str,
        conditions: List[FilterCondition] = None,
        meta_conditions: List[FilterCondition] = None,
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by Weaviate.
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
        {"operator": "And",
        "operands": [
            {
                "path": ["created_at"],
                "operator": "LessThan",
                "valueText": "2020-01-01",
            },
            {
                "path": ["created_at"],
                "operator": "GreaterThan",
                "valueInt": "2019-01-01",
            },
        ]}
        """
        table_name = table_name.capitalize()
        metadata_table_name = table_name.capitalize() + "_metadata"
        #
        if not (conditions or meta_conditions):
            return None

        # we translate each condition into a single dict
        #  conditions on columns
        weaviate_conditions = []
        if conditions:
            for condition in conditions:
                column_key = condition.column
                value_type = self._get_weaviate_value_type(condition.value)
                weaviate_conditions.append(
                    {
                        "path": [column_key],
                        "operator": self._get_weaviate_operator(condition.op),
                        value_type: condition.value,
                    }
                )
        # condition on metadata columns
        if meta_conditions:
            for condition in meta_conditions:
                meta_key = condition.column.split(".")[-1]
                value_type = self._get_weaviate_value_type(condition.value)
                weaviate_conditions.append(
                    {
                        "path": [
                            "associatedMetadata",
                            metadata_table_name,
                            meta_key,
                        ],
                        "operator": self._get_weaviate_operator(condition.op),
                        value_type: condition.value,
                    }
                )

        # we combine all conditions into a single dict
        all_conditions = (
            {"operator": "And", "operands": weaviate_conditions}
            # combining all conditions if there are more than one conditions
            if len(weaviate_conditions) > 1
            # only a single condition
            else weaviate_conditions[0]
        )
        return all_conditions

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ):
        table_name = table_name.capitalize()
        # columns which we will always provide in the result
        filters = None
        if conditions:
            non_metadata_conditions = [
                condition
                for condition in conditions
                if not condition.column.startswith(TableField.METADATA.value)
                and condition.column != TableField.SEARCH_VECTOR.value
                and condition.column != TableField.EMBEDDINGS.value
            ]
            metadata_conditions = [
                condition
                for condition in conditions
                if condition.column.startswith(TableField.METADATA.value)
            ]
            filters = self._translate_condition(
                table_name,
                non_metadata_conditions if non_metadata_conditions else None,
                metadata_conditions if metadata_conditions else None,
            )

        # check if embedding vector filter is present
        vector_filter = (
            None
            if not conditions
            else [
                condition
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
                or condition.column == TableField.EMBEDDINGS.value
            ]
        )

        for col in ["id", "embeddings", "distance", "metadata"]:
            if col in columns:
                columns.remove(col)

        metadata_table = table_name.capitalize() + "_metadata"

        metadata_fields = " ".join(
            [
                prop["name"]
                for prop in self._client.schema.get(metadata_table)["properties"]
            ]
        )

        # query to get all metadata fields
        metadata_query = (
            f"associatedMetadata {{ ... on {metadata_table} {{ {metadata_fields} }} }}"
        )

        if columns:
            query = self._client.query.get(
                table_name,
                columns + [metadata_query],
            ).with_additional(["id vector distance"])
        else:
            query = self._client.query.get(
                table_name,
                [metadata_query],
            ).with_additional(["id vector distance"])
        if vector_filter:
            # similarity search
            # assuming the similarity search is on content
            # assuming there would be only one vector based search per query
            vector_filter = vector_filter[0]
            near_vector = {
                "vector": ast.literal_eval(vector_filter.value)
                if isinstance(vector_filter.value, str)
                else vector_filter.value
            }
            query = query.with_near_vector(near_vector)
        if filters:
            query = query.with_where(filters)
        if limit:
            query = query.with_limit(limit)
        result = query.do()
        result = result["data"]["Get"][table_name.capitalize()]
        ids = [query_obj["_additional"]["id"] for query_obj in result]
        contents = [query_obj.get("content") for query_obj in result]
        distances = [
            query_obj.get("_additional").get("distance") for query_obj in result
        ]
        # distances will be null for non vector/embedding query
        vectors = [query_obj.get("_additional").get("vector") for query_obj in result]
        metadatas = [query_obj.get("associatedMetadata")[0] for query_obj in result]

        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: contents,
            TableField.METADATA.value: metadatas,
            TableField.EMBEDDINGS.value: vectors,
            TableField.DISTANCE.value: distances,
        }

        if columns:
            payload = {
                column: payload[column]
                for column in columns + ["id", "embeddings", "distance", "metadata"]
                if column != TableField.EMBEDDINGS.value
            }

        # always include distance
        if distances:
            payload[TableField.DISTANCE.value] = distances
        result_df = pd.DataFrame(payload)
        return result_df

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """
        Insert data into the Weaviate database.
        """

        table_name = table_name.capitalize()

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        data = data.to_dict(orient="records")
        # parsing the records one by one as we need to update metadata (which has variable columns)
        for record in data:
            metadata_data = record.get(TableField.METADATA.value)
            data_object = {"content": record.get(TableField.CONTENT.value)}
            data_obj_id = (
                record[TableField.ID.value]
                if TableField.ID.value in record.keys()
                else generate_uuid5(data_object)
            )
            obj_id = self._client.data_object.create(
                data_object=data_object,
                class_name=table_name,
                vector=record[TableField.EMBEDDINGS.value],
                uuid=data_obj_id,
            )
            if metadata_data:
                meta_id = self.add_metadata(metadata_data, table_name)
                self._client.data_object.reference.add(
                    from_uuid=obj_id,
                    from_property_name="associatedMetadata",
                    to_uuid=meta_id,
                )

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """
        Update data in the weaviate database.
        """
        table_name = table_name.capitalize()
        metadata_table_name = table_name.capitalize() + "_metadata"
        data_list = data.to_dict("records")
        for row in data_list:
            non_metadata_keys = [
                key
                for key in row.keys()
                if key and not key.startswith(TableField.METADATA.value)
            ]
            metadata_keys = [
                key.split(".")[1]
                for key in row.keys()
                if key and key.startswith(TableField.METADATA.value)
            ]

            id_filter = {"path": ["id"], "operator": "Equal", "valueText": row["id"]}
            metadata_id_query = f"associatedMetadata {{ ... on {metadata_table_name} {{ _additional {{ id }} }} }}"
            result = (
                self._client.query.get(table_name, metadata_id_query)
                .with_additional(["id"])
                .with_where(id_filter)
                .do()
            )

            metadata_id = result["data"]["Get"][table_name][0]["associatedMetadata"][0][
                "_additional"
            ]["id"][0]
            # updating table
            self._client.data_object.update(
                uuid=row["id"],
                class_name=table_name,
                data_object={key: row[key] for key in non_metadata_keys},
            )
            # updating metadata
            self._client.data_object.update(
                uuid=metadata_id,
                class_name=metadata_table_name,
                data_object={key: row[key] for key in metadata_keys},
            )

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        table_name = table_name.capitalize()
        non_metadata_conditions = [
            condition
            for condition in conditions
            if not condition.column.startswith(TableField.METADATA.value)
            and condition.column != TableField.SEARCH_VECTOR.value
            and condition.column != TableField.EMBEDDINGS.value
        ]
        metadata_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        filters = self._translate_condition(
            table_name,
            non_metadata_conditions if non_metadata_conditions else None,
            metadata_conditions if metadata_conditions else None,
        )
        if not filters:
            raise Exception("Delete query must have at least one condition!")
        metadata_table_name = table_name.capitalize() + "_metadata"
        # query to get metadata ids
        metadata_query = f"associatedMetadata {{ ... on {metadata_table_name} {{ _additional {{ id }} }} }}"
        result = (
            self._client.query.get(table_name, metadata_query)
            .with_additional(["id"])
            .with_where(filters)
            .do()
        )
        result = result["data"]["Get"][table_name]
        metadata_table_name = table_name.capitalize() + "_metadata"
        table_ids = []
        metadata_ids = []
        for i in result:
            table_ids.append(i["_additional"]["id"])
            metadata_ids.append(i["associatedMetadata"][0]["_additional"]["id"])
        self._client.batch.delete_objects(
            class_name=table_name,
            where={
                "path": ["id"],
                "operator": "ContainsAny",
                "valueTextArray": table_ids,
            },
        )
        self._client.batch.delete_objects(
            class_name=metadata_table_name,
            where={
                "path": ["id"],
                "operator": "ContainsAny",
                "valueTextArray": metadata_ids,
            },
        )

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Create a class with the given name in the weaviate database.
        """
        # separate metadata table for each table (as different tables will have different metadata columns)
        # this reduces the query time using metadata but increases the insertion time
        metadata_table_name = table_name + "_metadata"
        if not self._client.schema.exists(metadata_table_name):
            self._client.schema.create_class({"class": metadata_table_name})
        if not self._client.schema.exists(table_name):
            self._client.schema.create_class(
                {
                    "class": table_name,
                    "properties": [
                        {"dataType": ["text"], "name": prop["name"]}
                        for prop in self.SCHEMA
                        if prop["name"] != "id"
                        and prop["name"] != "embeddings"
                        and prop["name"] != "metadata"
                    ],
                    "vectorIndexType": "hnsw",
                }
            )
            add_prop = {
                "name": "associatedMetadata",
                "dataType": [metadata_table_name.capitalize()],
            }
            self._client.schema.property.create(table_name.capitalize(), add_prop)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Delete a class from the weaviate database.
        """
        table_name = table_name.capitalize()
        metadata_table_name = table_name.capitalize() + "_metadata"
        table_id_query = self._client.query.get(table_name).with_additional(["id"]).do()
        table_ids = [
            i["_additional"]["id"] for i in table_id_query["data"]["Get"][table_name]
        ]
        metadata_table_id_query = (
            self._client.query.get(metadata_table_name).with_additional(["id"]).do()
        )
        metadata_ids = [
            i["_additional"]["id"]
            for i in metadata_table_id_query["data"]["Get"][metadata_table_name]
        ]
        self._client.batch.delete_objects(
            class_name=table_name,
            where={
                "path": ["id"],
                "operator": "ContainsAny",
                "valueTextArray": table_ids,
            },
        )
        self._client.batch.delete_objects(
            class_name=metadata_table_name,
            where={
                "path": ["id"],
                "operator": "ContainsAny",
                "valueTextArray": metadata_ids,
            },
        )
        try:
            self._client.schema.delete_class(table_name)
            self._client.schema.delete_class(metadata_table_name)
        except ValueError:
            if not if_exists:
                raise Exception(f"Table {table_name} does not exist!")

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of tables in the Weaviate database.
        """
        query_tables = self._client.schema.get()
        tables = []
        if query_tables:
            tables = [table["class"] for table in query_tables["classes"]]
        table_name = pd.DataFrame(
            columns=["table_name"],
            data=tables,
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=table_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        table_name = table_name.capitalize()
        # check if table exists
        try:
            table = self._client.schema.get(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        data = pd.DataFrame(
            data=[
                {"COLUMN_NAME": column["name"], "DATA_TYPE": column["dataType"][0]}
                for column in table["properties"]
            ]
        )
        return Response(data_frame=data, resp_type=RESPONSE_TYPE.OK)

    def add_metadata(self, data: dict, table_name: str):
        table_name = table_name.capitalize()
        metadata_table_name = table_name.capitalize() + "_metadata"
        self._client.schema.get(metadata_table_name)
        # getting existing metadata fields
        added_prop_list = [
            prop["name"]
            for prop in self._client.schema.get(metadata_table_name)["properties"]
        ]
        # as metadata columns are not fixed, at every entry, a check takes place for the columns
        for prop in data.keys():
            if prop not in added_prop_list:
                if isinstance(data[prop], int):
                    add_prop = {
                        "name": prop,
                        "dataType": ["int"],
                    }
                elif isinstance(data[prop][0], datetime):
                    add_prop = {
                        "name": prop,
                        "dataType": ["date"],
                    }
                else:
                    add_prop = {
                        "name": prop,
                        "dataType": ["string"],
                    }
                # when a new column is identified, it is added to the metadata table
                self._client.schema.property.create(metadata_table_name, add_prop)
        metadata_id = self._client.data_object.create(
            data_object=data, class_name=table_name.capitalize() + "_metadata"
        )
        return metadata_id
