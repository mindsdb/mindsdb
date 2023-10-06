from collections import OrderedDict
from datetime import datetime
from typing import List, Optional

import weaviate
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


class WeaviateDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Weaviate statements."""

    name = "weaviate"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)

        self._connection_data = kwargs.get("connection_data")

        self._client_config = {
            "weaviate_url": self._connection_data.get("weaviate_url"),
            "weaviate_api_key": self._connection_data.get(
                "weaviate_api_key"
            ),
            "persist_directory": self._connection_data.get("persist_directory"),
        }

        # either host + port or persist_directory is required
        # but not both
        if (
                self._client_config["weaviate_url"] is None
                or self._client_config["weaviate_api_key"] is None
        ):
            raise Exception(
                "Either url + auth client secret is required for weaviate connection!"
            )

        self._client = None
        self.is_connected = False
        self.connect()

    def _get_client(self) -> weaviate.Client:
        client_config = self._client_config
        if client_config is None:
            raise Exception("Client config is not set!")

        # decide the client type to be used, either persistent or httpclient
        return weaviate.Client(
            url=client_config["weaviate_url"],
            auth_client_secret=weaviate.AuthApiKey(api_key=client_config["weaviate_api_key"]),
        )

    def __del__(self):
        if self._client is not None:
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
            log.logger.error(f"Error connecting to weaviate client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""

        if not self.is_connected:
            return

        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Weaviate database."""
        response_code = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            if self._client.is_live():
                response_code.success = True
        except Exception as e:
            log.logger.error(f"Error connecting to weaviate , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success and need_to_close:
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
            FilterOperator.LIKE: "Like"
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by weaviate!")

        return mapping[operator]

    @staticmethod
    def _get_weaviate_value_type(value) -> str:
        value_type = None,
        if isinstance(value, list):
            value_list_types = {
                str: "valueStringList",
                int: "valueIntList",
                float: "valueIntList",
                bool: "valueBooleanList"}
            value_type = value_list_types.get(type(value[0]))

        else:
            value_primitive_types = {
                str: "valueString",
                int: "valueInt",
                float: "valueInt",
                datetime: "valueDate",
                bool: "valueBoolean",
            }
            value_type = value_primitive_types.get(type(value))

        if value_type is None:
            raise Exception(f"Value type {type(value)} is not supported by weaviate!")

        return value_type

    def _translate_condition(
            self, table_name: str, conditions: List[FilterCondition] = None, meta_conditions: List[FilterCondition] = None,
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
        {
            "And": [
                {"created_at": {"$lt": "2020-01-01"}},
                {"created_at": {"$gt": "2019-01-01"}}
            ]
        }
        """
        # we ignore all non-metadata conditions
        if conditions is None and meta_conditions is None:
            return None

        # we translate each condition into a dict
        weaviate_conditions = []
        if conditions is not None:

            for condition in conditions:
                column_key = condition.column
                value_type = self._get_weaviate_value_type(condition.value)
                weaviate_conditions.append(
                    {
                        'path': [column_key],
                        'operator': self._get_weaviate_operator(condition.op),
                        value_type: condition.value
                    }
                )
        if meta_conditions is not None:
            for condition in meta_conditions:
                meta_key = condition.column.split(".")[-1]
                value_type = self._get_weaviate_value_type(condition.value)
                weaviate_conditions.append(
                    {
                        'path': ["associatedMetadata", table_name.capitalize() + "_metadata", meta_key],
                        'operator': self._get_weaviate_operator(condition.op),
                        value_type: condition.value
                    }
                )

        # we combine all conditions into a single dict
        all_conditions = (
            {'operator': "And",
             'operands': weaviate_conditions}
            if len(weaviate_conditions) > 1
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
    ) -> HandlerResponse:

        fixed_columns = ["id", "embeddings", "distance", "metadata"]
        filters = None
        if conditions is not None:
            filters = self._translate_condition(table_name, [condition for condition in conditions
                                                 if not condition.column.startswith(TableField.METADATA.value)],
                                                [condition for condition in conditions
                                                 if condition.column.startswith(TableField.METADATA.value)])

        # check if embedding vector filter is present
        vector_filter = (
            []
            if conditions is None
            else [
                condition
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value or
                   condition.column == TableField.EMBEDDINGS.value
            ]
        )

        if len(vector_filter) > 0:
            vector_filter = vector_filter[0]
        else:
            vector_filter = None

        for col in fixed_columns:
            if col in columns:
                columns.remove(col)
        metadata_fields = " ".join(
            [prop['name'] for prop in self._client.schema.get(table_name.capitalize() + "_metadata")['properties']])
        if columns is not None and len(columns) > 0:
            query = self._client.query.get(table_name, columns + [
                "associatedMetadata {... on " + table_name.capitalize() + "_metadata { " + metadata_fields + " }}"]).with_additional(
                ["id vector distance"])
        else:
            query = self._client.query.get(table_name, [
                "associatedMetadata {... on " + table_name.capitalize() + "_metadata { " + metadata_fields + " }}"]).with_additional(
                ["id vector distance"])
        if vector_filter is not None:
            # similarity search
            # assuming the similarity search is on content
            near_vector = {
                "vector": vector_filter.value
            }
            query = query.with_near_vector(near_vector)
        if filters is not None:
            query = query.with_where(filters)
        if limit is not None:
            query = query.with_limit(limit)
        result = query.do()
        result = result['data']['Get'][table_name.capitalize()]
        ids = [query_obj['_additional']["id"] for query_obj in result]
        contents = [query_obj.get("content") for query_obj in result]
        distances = [query_obj.get('_additional').get("distance") for query_obj in result]
        vectors = [query_obj.get('_additional').get("vector") for query_obj in result]
        metadatas = [query_obj.get("associatedMetadata") for query_obj in result]

        # project based on columns
        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: contents,
            TableField.METADATA.value: metadatas,
            TableField.EMBEDDINGS.value: vectors,
            TableField.SEARCH_VECTOR.value: vectors,
            TableField.DISTANCE.value: distances
        }

        if columns is not None:
            payload = {
                column: payload[column]
                for column in columns + fixed_columns
                if column != TableField.EMBEDDINGS.value
            }

        # always include distance
        if distances is not None:
            payload[TableField.DISTANCE.value] = distances
        result_df = pd.DataFrame(payload)
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result_df)

    def insert(
            self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Insert data into the Weaviate database.
        """

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        data = data.to_dict(orient="records")
        for record in data:
            metadata_data = record.get(TableField.METADATA.value)
            meta_id = self.add_metadata(metadata_data, table_name)
            obj_id = self._client.data_object.create(data_object={"content": record.get(TableField.CONTENT.value)},
                                                class_name=table_name,
                                                vector=record[TableField.EMBEDDINGS.value])
            self._client.data_object.reference.add(
                from_uuid=obj_id,
                from_property_name='associatedMetadata',
                to_uuid=meta_id
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def update(
            self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Update data in the weaviate database.
        """
        dict_list = data.to_dict('records')
        for row in dict_list:
            self._client.data_object.update(
                uuid=row["id"],
                class_name=table_name,
                data_object={
                    key: row[key]
                    for key in row.keys()
                },
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def delete(
            self, table_name: str, conditions: List[FilterCondition] = None
    ) -> HandlerResponse:
        filters = self._translate_condition(table_name, conditions)

        if filters is None:
            raise Exception("Delete query must have at least one condition!")
        result = self._client.query.get(table_name,
                                        "associatedMetadata { ... on " + table_name.capitalize() + "_metadata { id } }").with_additional(
            ["id"]).with_where(filters).do()
        result = result['data']["Get"][table_name]
        for query_obj in result:
            obj_id = query_obj['_additional']["id"]
            meta_id = query_obj[table_name.capitalize() + '_metadata']["id"]
            self._client.data_object.reference.delete(
                from_class_name=table_name,
                from_uuid=obj_id,
                from_property_name="associatedMetadata",
                to_class_name=table_name.capitalize() + "_metadata",
                to_uuid=meta_id,
            )
            self._client.data_object.delete(
                uuid=meta_id,
                class_name='Metadata',
            )
            self._client.data_object.delete(
                uuid=obj_id,
                class_name=table_name,
            )
        return Response(resp_type=RESPONSE_TYPE.OK)

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """
        Create a class with the given name in the weaviate database.
        """
        if not self._client.schema.exists(table_name.capitalize() + '_metadata'):
            self._client.schema.create_class({"class": table_name.capitalize() + '_metadata'})
        if not self._client.schema.exists(table_name):
            self._client.schema.create_class({"class": table_name,
                                              'properties': [{"dataType": ['text'],
                                                              "name": prop['name']} for prop in self.SCHEMA
                                                             if prop["name"] != "id" and prop[
                                                                 'name'] != 'embeddings' and prop[
                                                                 'name'] != 'metadata'],
                                              "vectorIndexType": "hnsw"
                                              })
            add_prop = {
                'name': 'associatedMetadata',
                'dataType': [table_name.capitalize() + '_metadata'],
            }
            self._client.schema.property.create(table_name, add_prop)

        return Response(resp_type=RESPONSE_TYPE.OK)

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """
        Delete a class from the weaviate database.
        """
        try:
            self._client.schema.delete_class(table_name)
        except ValueError:
            if if_exists:
                return Response(resp_type=RESPONSE_TYPE.OK)
            else:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message=f"Table {table_name} does not exist!",
                )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of classes in the Weaviate database.
        """
        classes = self._client.schema.get()["classes"]

        classes_name = pd.DataFrame(
            columns=["table_name"],
            data=[classes["class"] for classes in classes],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=classes_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        # check if class exists
        try:
            weaviate_class = self._client.schema.get(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        data = pd.DataFrame(data=[{"COLUMN_NAME": column["name"],
                                   "DATA_TYPE": column["dataType"][0]} for column in weaviate_class["properties"]])
        return Response(
            data_frame=data,
            resp_type=RESPONSE_TYPE.OK
        )

    def add_metadata(self, data: dict, table_name: str):
        self._client.schema.get(table_name.capitalize() + '_metadata')
        added_prop_list = [prop['name'] for prop in
                           self._client.schema.get(table_name.capitalize() + '_metadata')['properties']]
        for prop in data.keys():
            if prop not in added_prop_list:
                if isinstance(data[prop], int):
                    add_prop = {
                        'name': prop,
                        'dataType': ['int'],
                    }
                elif isinstance(data[prop][0], datetime):
                    add_prop = {
                        'name': prop,
                        'dataType': ['date'],
                    }
                else:
                    add_prop = {
                        'name': prop,
                        'dataType': ['string'],
                    }
                self._client.schema.property.create(table_name.capitalize() + '_metadata', add_prop)

        metadata_id = self._client.data_object.create(data_object=data,
                                                      class_name=table_name.capitalize() + '_metadata')
        return metadata_id


connection_args = OrderedDict(
    weaviate_url={
        "type": ARG_TYPE.STR,
        "description": "weaviate url",
        "required": True,
    },
    weaviate_api_key={
        "type": ARG_TYPE.STR,
        "description": "weaviate API KEY",
        "required": True,
    },
)

connection_args_example = OrderedDict(
    weaviate_url="https://sample-s3x8blpm.weaviate.network",
    weaviate_api_key="mdMdLfFSbwcWqLI6NUNUseIoIS8yxnY84U5C",
)
