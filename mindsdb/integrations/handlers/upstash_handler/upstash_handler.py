import json
from typing import List
from upstash_vector import Index, Vector
import pandas as pd

from mindsdb.integrations.libs.vectordatabase_handler import (
    VectorStoreHandler,
    TableField,
    FilterCondition,
    FilterOperator,
)
from mindsdb.integrations.libs.response import (
    RESPONSE_TYPE,
    HandlerResponse,
    HandlerStatusResponse,
)

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class UpstashHandler(VectorStoreHandler):
    """This handler handles connection and execution of the Upstash Vector statements."""

    name = "upstash"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.MAX_FETCH_LIMIT = 1000
        self._connection_data = kwargs.get("connection_data")
        self._client_config = {
            "url": self._connection_data.get("url"),
            "token": self._connection_data.get("token"),
        }
        self._table_create_params = {
            "retries": 3,
            "retry_interval": 1.0,
        }
        for key in self._table_create_params:
            if key in self._connection_data:
                self._table_create_params[key] = self._connection_data[key]
        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _get_upstash_operator(self, operator: FilterOperator) -> str:
        """Converts a FilterOperator to an equivalent Upstash Vector operator."""
        if operator == FilterOperator.EQUAL:
            return "="
        elif operator == FilterOperator.NOT_EQUAL:
            return "!="
        elif operator == FilterOperator.GREATER_THAN:
            return ">"
        elif operator == FilterOperator.GREATER_THAN_OR_EQUAL:
            return ">="
        elif operator == FilterOperator.LESS_THAN:
            return "<"
        elif operator == FilterOperator.LESS_THAN_OR_EQUAL:
            return "<="
        else:
            raise Exception(f"Unsupported operator: {operator}")

    def _get_index_handle(self, url, token):
        """Returns handler to index specified by `url` and `token`."""
        index = Index(url=url, token=token)
        try:
            index.info()
        except Exception as e:
            logger.error(f"Error while fetching index info: {e}")
            index = None
        return index

    # You can only connect to an existing index in the Upstash Console
    def connect(self):
        """Connect to an Upstash Vector database."""
        try:
            Index(
                url=self._client_config["url"],
                token=self._client_config["token"],
                retries=self._table_create_params["retries"],
                retry_interval=self._table_create_params["retry_interval"],
            )
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to Upstash Vector: {e}")
            self.is_connected = False
        return self.check_connection()

    # No need to disconnect from Upstash Vector SDK since it's a REST API
    def disconnect(self):
        """Disconnect from an Upstash Vector database."""
        if self.is_connected is False:
            return
        self.is_connected = False

    def check_connection(self):
        """Check if the connection to the Upstash Vector database is alive."""
        response_code = HandlerStatusResponse(False)
        try:
            index = self._get_index_handle(
                self._client_config["url"], self._client_config["token"]
            )
            index.info()
            if self.is_connected:
                response_code.success = True
        except Exception as e:
            logger.error(f"Error while checking connection to Upstash Vector: {e}")
            response_code.error_message = str(e)
        return response_code

    def get_tables(self) -> HandlerResponse:
        """Get the list of namespaces and their vector counts from the Upstash database."""
        logger.info("Use the Upstash Console to see the list of indexes.")

        index = self._get_index_handle(
            self._client_config["url"], self._client_config["token"]
        )
        if index is None:
            raise Exception("Error getting index, are you sure the name is correct?")
        info_result = index.info()

        # Create a DataFrame to store namespace information
        namespaces_data = [
            {
                "namespace": ns,
                "vector_count": ns_info.vector_count,
                "pending_vector_count": ns_info.pending_vector_count,
            }
            for ns, ns_info in info_result.namespaces.items()
        ]
        namespaces_df = pd.DataFrame(namespaces_data)
        return HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=namespaces_df)

    # You can only create an index in the Upstash Console
    def create_table(
        self, table_name: str, retries: int = 3, retry_interval: float = 1.0
    ):
        """Directs the user to create an index in the Upstash Console."""
        try:
            index = Index(
                url=self._client_config["url"],
                token=self._client_config["token"],
                retries=retries,
                retry_interval=retry_interval,
            )
            index.info()
        except Exception as e:
            logger.error(f"Error while creating table '{table_name}': {e}")
            logger.info("Use the Upstash Console to create an index if it doesn't exist.")

    # You can only delete an index in the Upstash Console
    def drop_table(self, table_name: str):
        """Clear all vectors and metadata from a particular namespace"""
        logger.info("Use the Upstash Console to delete the your entire index.")
        index = self._get_index_handle(
            self._client_config["url"], self._client_config["token"]
        )
        if index is None:
            raise Exception(
                f"Error getting index '{table_name}', are you sure the name is correct?"
            )
        try:
            index.reset(namespace=table_name)
        except Exception as e:
            raise Exception(f"Failed to delete index '{table_name}': {str(e)}")

    def insert(self, table_name: str, data: pd.DataFrame, columns: List[str] = None):
        """Insert data into an Upstash Vector index."""
        upsert_batch_size = 99

        index = self._get_index_handle(
            self._client_config["url"], self._client_config["token"]
        )
        if index is None:
            raise Exception(
                f"Error getting index '{table_name}', are you sure the name is correct?"
            )

        data.rename(
            columns={
                TableField.ID.value: "id",
                TableField.EMBEDDINGS.value: "vector",
                TableField.METADATA.value: "metadata",
                TableField.CONTENT.value: "data",
            },
            inplace=True,
        )
        data = data[["id", "vector", "metadata", "data"]]

        # Preprocess the 'vector' column to ensure all values are lists
        data["vector"] = data["vector"].apply(
            lambda v: eval(v) if isinstance(v, str) else v
        )

        # Preprocess the 'metadata' column to ensure all values are valid JSON objects
        data["metadata"] = data["metadata"].apply(
            lambda m: json.loads(m) if isinstance(m, str) else m
        )

        # Split the data into batches for upsert
        for batch_start in range(0, len(data), upsert_batch_size):
            batch = data.iloc[batch_start: batch_start + upsert_batch_size]

            # Convert the batch rows into Vector objects
            vectors = [
                Vector(
                    id=row["id"],
                    vector=row["vector"],
                    metadata=row["metadata"],
                    data=row["data"],
                )
                for _, row in batch.iterrows()
            ]

            # Perform the upsert operation
            try:
                index.upsert(vectors=vectors, namespace=table_name)
            except Exception as e:
                raise Exception(
                    f"Failed to upsert vectors for batch starting at index {batch_start}: {str(e)}"
                )

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        """Delete vectors from the index based on their identifiers."""
        ids = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ] or None

        if ids is None:
            raise Exception("No IDs provided for deletion.")

        index = self._get_index_handle(
            self._client_config["url"], self._client_config["token"]
        )

        if index is None:
            raise Exception(
                f"Error getting index '{table_name}', are you sure the name is correct?"
            )

        try:
            index.delete(ids=ids, namespace=table_name)
        except Exception as e:
            raise Exception(f"Failed to delete vectors: {str(e)}")

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select vectors from the index based on their identifiers."""
        index = self._get_index_handle(
            self._client_config["url"], self._client_config["token"]
        )
        if index is None:
            raise Exception(
                f"Error getting index '{table_name}', are you sure the name is correct?"
            )

        # check for id filter
        id_filters = None
        search_vector = None
        metadata_filters = None
        if conditions is not None:
            for condition in conditions:
                condition_val = condition.value
                if type(condition.value) is str:
                    # Check if the string is in a format that can be safely evaluated
                    if condition.value.strip().startswith(("[", "{")):
                        condition_val = eval(condition.value)
                if condition.column == TableField.ID.value:
                    id_filters = condition_val
                    break
                elif condition.column == TableField.SEARCH_VECTOR.value:
                    search_vector = condition_val
                elif condition.column.startswith(TableField.METADATA.value):
                    # Extract the part before the dot
                    metadata_key = condition.column.split(".")[
                        1
                    ]  # This will give 'metadata key'
                    operator = self._get_upstash_operator(condition.op)
                    # Check if there are multiple metadata filters
                    if metadata_filters is not None:
                        metadata_filters += (
                            f" AND {metadata_key} {operator} '{condition_val}'"
                        )
                    metadata_filters = f"{metadata_key} {operator} '{condition_val}'"

        # Fetch vectors if ids are provided
        if id_filters is not None:
            vectors = index.fetch(
                ids=id_filters,
                include_data=True,
                include_metadata=True,
                include_vectors=True,
                namespace=table_name,
            )
        # Query the index for vectors based on search_vector_filters
        elif search_vector is not None:
            vectors = index.query(
                vector=search_vector,
                include_data=True,
                include_metadata=True,
                include_vectors=True,
                namespace=table_name,
                top_k=(
                    limit if limit is not None else 10
                ),  # if limit is None, set top_k to 5
                filter=metadata_filters if metadata_filters is not None else None,
            )
        else:
            raise Exception("No filters provided for selection.")

        if len(vectors) == 0:
            return pd.DataFrame()

        if limit is not None and len(vectors) > limit:
            vectors = vectors[:limit]

        # Convert the fetched vectors into a DataFrame
        vectors_data = [
            {
                TableField.ID.value: v.id,
                TableField.EMBEDDINGS.value: v.vector,
                TableField.METADATA.value: json.dumps(v.metadata),
                TableField.CONTENT.value: v.data,
            }
            for v in vectors
        ]

        # if score is present in the response, add it to the DataFrame
        if hasattr(vectors[0], "score"):
            for i, v in enumerate(vectors):
                vectors_data[i]["score"] = v.score

        vectors_df = pd.DataFrame(vectors_data)
        return vectors_df

    def get_columns(self, table_name: str) -> HandlerResponse:
        return super().get_columns(table_name)
