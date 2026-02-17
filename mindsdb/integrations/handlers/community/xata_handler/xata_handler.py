from typing import List, Optional

import pandas as pd
import json
import xata
from xata.helpers import BulkProcessor

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


class XataHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Xata statements."""

    name = "xata"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs.get("connection_data")
        self._client_config = {
            "db_url": self._connection_data.get("db_url"),
            "api_key": self._connection_data.get("api_key"),
        }
        self._create_table_params = {
            "dimension": self._connection_data.get("dimension", 8),
        }
        self._select_params = {
            "similarity_function": self._connection_data.get("similarity_function", "cosineSimilarity"),
        }
        self._client = None
        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """Connect to a Xata database."""
        if self.is_connected is True:
            return self._client
        try:
            self._client = xata.XataClient(**self._client_config)
            self.is_connected = True
            return self._client
        except Exception as e:
            logger.error(f"Error connecting to Xata client: {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected is False:
            return
        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the Xata database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False
        # NOTE: no direct way to test this
        # try getting the user, if it fails, it means that we are not connected
        try:
            resp = self._client.users().get()
            if not resp.is_success():
                raise Exception(resp["message"])
            response_code.success = True
        except Exception as e:
            logger.error(f"Error connecting to Xata: {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False
        return response_code

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """Create a table with the given name in the Xata database."""

        resp = self._client.table().create(table_name)
        if not resp.is_success():
            raise Exception(f"Unable to create table {table_name}: {resp['message']}")
        resp = self._client.table().set_schema(
            table_name=table_name,
            payload={
                "columns": [
                    {
                        "name": "embeddings",
                        "type": "vector",
                        "vector": {"dimension": self._create_table_params["dimension"]}
                    },
                    {"name": "content", "type": "text"},
                    {"name": "metadata", "type": "json"},
                ]
            }
        )
        if not resp.is_success():
            raise Exception(f"Unable to change schema of table {table_name}: {resp['message']}")

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """Delete a table from the Xata database."""

        resp = self._client.table().delete(table_name)
        if not resp.is_success():
            raise Exception(f"Unable to delete table: {resp['message']}")

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get columns of the given table"""
        # Vector stores have predefined columns
        try:
            # But at least try to see if the table is valid
            resp = self._client.table().get_columns(table_name)
            if not resp.is_success():
                raise Exception(f"Error getting columns: {resp['message']}")
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"{e}",
            )
        return super().get_columns(table_name)

    def get_tables(self) -> HandlerResponse:
        """Get the list of tables in the Xata database."""
        try:
            table_names = pd.DataFrame(
                columns=["TABLE_NAME"],
                data=[table_data["name"] for table_data in self._client.branch().get_details()["schema"]["tables"]],
            )
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=table_names)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting list of tables: {e}",
            )

    def insert(self, table_name: str, data: pd.DataFrame, columns: List[str] = None):
        """ Insert data into the Xata database. """
        if columns:
            data = data[columns]
        # Convert to records
        data = data.to_dict("records")
        # Convert metadata to json
        for row in data:
            if "metadata" in row:
                row["metadata"] = json.dumps(row["metadata"])
        if len(data) > 1:
            # Bulk processing
            bp = BulkProcessor(self._client, throw_exception=True)
            bp.put_records(table_name, data)
            bp.flush_queue()

        elif len(data) == 0:
            # Skip
            return Response(resp_type=RESPONSE_TYPE.OK)
        elif "id" in data[0] and TableField.ID.value in columns:
            # If id present
            id = data[0]["id"]
            rest_of_data = data[0].copy()
            del rest_of_data["id"]

            resp = self._client.records().insert_with_id(
                table_name=table_name,
                record_id=id,
                payload=rest_of_data,
                create_only=True,
                columns=columns
            )
            if not resp.is_success():
                raise Exception(resp["message"])

        else:
            # If id not present
            resp = self._client.records().insert(
                table_name=table_name,
                payload=data[0],
                columns=columns
            )
            if not resp.is_success():
                raise Exception(resp["message"])

    def update(self, table_name: str, data: pd.DataFrame, columns: List[str] = None) -> HandlerResponse:
        """Update data in the Xata database."""
        # Not supported
        return super().update(table_name, data, columns)

    def _get_xata_operator(self, operator: FilterOperator) -> str:
        """Translate SQL operator to oprator understood by Xata filter language."""
        mapping = {
            FilterOperator.EQUAL: "$is",
            FilterOperator.NOT_EQUAL: "$isNot",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$le",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
            FilterOperator.LIKE: "$pattern",
        }
        if operator not in mapping:
            raise Exception(f"Operator '{operator}' is not supported!")
        return mapping[operator]

    def _translate_non_vector_conditions(self, conditions: List[FilterCondition]) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by Xata for filtering.
        E.g.,
        [
            FilterCondition(
                column="metadata.price",
                op=FilterOperator.LESS_THAN,
                value=100,
            ),
            FilterCondition(
                column="metadata.price",
                op=FilterOperator.GREATER_THAN,
                value=10,
            )
        ]
        -->
        {
            "metadata->price" {
                "$gt": 10,
                "$lt": 100
            },
        }
        """
        if not conditions:
            return None
        # Translate metadata columns
        for condition in conditions:
            if condition.column.startswith(TableField.METADATA.value):
                condition.column = condition.column.replace(".", "->")
        # Generate filters
        filters = {}
        for condition in conditions:
            # Skip search vector condition
            if condition.column == TableField.SEARCH_VECTOR.value:
                continue
            current_filter = original_filter = {}
            # Special case LIKE: needs pattern translation
            if condition.op == FilterOperator.LIKE:
                condition.value = condition.value.replace("%", "*").replace("_", "?")
            # Generate substatment
            current_filter[condition.column] = {self._get_xata_operator(condition.op): condition.value}
            # Check for conflicting and insert
            for key in original_filter:
                if key in filters:
                    filters[key] = {**filters[key], **original_filter[key]}
                else:
                    filters = {**filters, **original_filter}
        return filters if filters else None

    def select(self, table_name: str, columns: List[str] = None, conditions: List[FilterCondition] = None,
               offset: int = None, limit: int = None) -> pd.DataFrame:
        """Run general query or a vector similarity search and return results."""
        if not columns:
            columns = [col["name"] for col in self.SCHEMA]
        # Generate filter conditions
        filters = self._translate_non_vector_conditions(conditions)
        # Check for search vector
        search_vector = (
            []
            if conditions is None
            else [
                condition.value
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )
        if len(search_vector) > 0:
            search_vector = search_vector[0]
        else:
            search_vector = None
        # Search
        results_df = pd.DataFrame(columns)
        if search_vector is not None:
            # Similarity

            params = {
                "queryVector": search_vector,
                "column": TableField.EMBEDDINGS.value,
                "similarityFunction": self._select_params["similarity_function"]
            }
            if filters:
                params["filter"] = filters
            if limit:
                params["size"] = limit
            results = self._client.data().vector_search(table_name, params)
            # Check for errors
            if not results.is_success():
                raise Exception(results["message"])
            # Convert result
            results_df = pd.DataFrame.from_records(results["records"])
            if "xata" in results_df.columns:
                results_df["xata"] = results_df["xata"].apply(lambda x: x["score"])
                results_df.rename({"xata": TableField.DISTANCE.value}, axis=1, inplace=True)

        else:
            # General get query

            params = {
                "columns": columns if columns else [],
            }
            if filters:
                params["filter"] = filters
            if limit or offset:
                params["page"] = {}
                if limit:
                    params["page"]["size"] = limit
                if offset:
                    params["page"]["offset"] = offset
            results = self._client.data().query(table_name, params)
            # Check for errors
            if not results.is_success():
                raise Exception(results["message"])
            # Convert result
            results_df = pd.DataFrame.from_records(results["records"])
            if "xata" in results_df.columns:
                results_df.drop(["xata"], axis=1, inplace=True)

        return results_df

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        ids = []
        for condition in conditions:
            if condition.op == FilterOperator.EQUAL:
                ids.append(condition.value)
            else:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message="You can only delete using '=' operator ID one at a time!",
                )

        for id in ids:
            resp = self._client.records().delete(table_name, id)
            if not resp.is_success():
                raise Exception(resp["message"])
