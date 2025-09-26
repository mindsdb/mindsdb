from typing import List, Optional
import json
import numpy as np

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


class DeepLakeHandler(VectorStoreHandler):
    """Handler for Deep Lake vector database operations."""

    name = "deeplake"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.deeplake_client = None
        self.dataset = None
        self._connection_data = kwargs["connection_data"]

        # Extract search parameters
        self._search_limit = self._connection_data.get("search_default_limit", 10)
        self._search_distance_metric = self._connection_data.get("search_distance_metric", "cosine")
        self._search_exec_option = self._connection_data.get("search_exec_option", "python")

        # Extract creation parameters
        self._create_overwrite = self._connection_data.get("create_overwrite", False)
        self._create_embedding_dim = self._connection_data.get("create_embedding_dim", 384)
        self._create_max_chunk_size = self._connection_data.get("create_max_chunk_size", 1000)
        self._create_compression = self._connection_data.get("create_compression", None)

        # Extract runtime configuration
        self._runtime = self._connection_data.get("runtime", {})

        self.is_connected = False
        self.connect()

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def connect(self):
        """Connect to Deep Lake dataset."""
        if self.is_connected:
            return

        try:
            import deeplake

            self.deeplake_client = deeplake

            # Extract connection parameters
            dataset_path = self._connection_data["dataset_path"]
            token = self._connection_data.get("token")
            read_only = self._connection_data.get("read_only", False)

            # Connect to dataset
            connect_kwargs = {
                "path": dataset_path,
                "read_only": read_only,
            }

            if token:
                connect_kwargs["token"] = token

            if self._runtime:
                connect_kwargs["runtime"] = self._runtime

            # Try to open existing dataset first
            try:
                self.dataset = deeplake.load(dataset_path, token=token, read_only=read_only)
            except Exception:
                # Dataset doesn't exist, will be created when needed
                self.dataset = None

            self.is_connected = True

        except Exception as e:
            logger.error(f"Error connecting to Deep Lake: {e}")
            self.is_connected = False

    def disconnect(self):
        """Disconnect from Deep Lake dataset."""
        if not self.is_connected:
            return

        if self.dataset:
            try:
                # Deep Lake datasets don't need explicit disconnection
                self.dataset = None
            except Exception as e:
                logger.warning(f"Error during disconnection: {e}")

        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """Check connection to Deep Lake."""
        response_code = StatusResponse(False)
        try:
            response_code.success = self.is_connected and self.deeplake_client is not None
        except Exception as e:
            logger.error(f"Error checking Deep Lake connection: {e}")
            response_code.error_message = str(e)
        return response_code

    def get_tables(self) -> HandlerResponse:
        """Get list of available datasets/tables."""
        try:
            # For Deep Lake, we consider each dataset as a table
            # If dataset exists, return it as a table, otherwise return empty
            if self.dataset is not None:
                table_name = self._connection_data["dataset_path"].split("/")[-1]
                if not table_name:
                    table_name = "default"
                tables_df = pd.DataFrame({"TABLE_NAME": [table_name]})
            else:
                tables_df = pd.DataFrame({"TABLE_NAME": []})

            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=tables_df)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting tables: {e}",
            )

    def drop_table(self, table_name: str, if_exists=True):
        """Delete a Deep Lake dataset."""
        try:
            if self.dataset is not None:
                # Deep Lake doesn't have a direct drop method
                # We would need to delete the dataset path
                if not if_exists:
                    raise Exception("Cannot drop Deep Lake dataset through handler")
                logger.warning("Deep Lake dataset deletion should be done through file system")
        except Exception as e:
            if not if_exists:
                raise Exception(f"Error dropping table '{table_name}': {e}")

    def _get_deeplake_operator(self, operator: FilterOperator) -> str:
        """Map FilterOperator to Deep Lake filter format."""
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
            raise Exception(f"Operator {operator} is not supported by Deep Lake!")
        return mapping[operator]

    def _translate_conditions(self, conditions: Optional[List[FilterCondition]]) -> Optional[dict]:
        """Translate FilterCondition objects to Deep Lake filter format."""
        if not conditions:
            return None

        # Deep Lake uses dictionary filters for metadata
        filter_dict = {}

        for condition in conditions:
            if condition.column.startswith(TableField.METADATA.value):
                # Extract metadata field name
                field_name = condition.column.split(".")[-1]

                # Build filter condition
                if condition.op == FilterOperator.EQUAL:
                    filter_dict[field_name] = condition.value
                elif condition.op == FilterOperator.IN:
                    filter_dict[field_name] = {"$in": condition.value}
                elif condition.op == FilterOperator.NOT_EQUAL:
                    filter_dict[field_name] = {"$ne": condition.value}
                elif condition.op == FilterOperator.GREATER_THAN:
                    filter_dict[field_name] = {"$gt": condition.value}
                elif condition.op == FilterOperator.LESS_THAN:
                    filter_dict[field_name] = {"$lt": condition.value}
                # Add more operators as needed

        return filter_dict if filter_dict else None

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ):
        """Select data from Deep Lake dataset."""
        if not self.dataset:
            raise Exception(f"Dataset {table_name} does not exist")

        # Find vector search condition
        vector_filter = None
        if conditions:
            for condition in conditions:
                if condition.column == TableField.SEARCH_VECTOR.value:
                    vector_filter = condition.value
                    break

        # Set default limit
        if limit is None:
            limit = self._search_limit

        try:
            if vector_filter is not None:
                # Vector similarity search
                if isinstance(vector_filter, str):
                    vector_filter = json.loads(vector_filter)
                if isinstance(vector_filter, list):
                    vector_filter = np.array(vector_filter)

                # Prepare search parameters
                search_kwargs = {
                    "k": limit,
                    "distance_metric": self._search_distance_metric,
                    "exec_option": self._search_exec_option,
                }

                # Add metadata filter if present
                metadata_filter = self._translate_conditions(conditions)
                if metadata_filter:
                    search_kwargs["filter"] = metadata_filter

                # Perform similarity search using the embedding field
                if hasattr(self.dataset, "embedding") and len(self.dataset.embedding) > 0:
                    results = self.dataset.embedding.search(query_vector=vector_filter, **search_kwargs)

                    # Process results
                    data = {
                        TableField.ID.value: [],
                        TableField.CONTENT.value: [],
                        TableField.EMBEDDINGS.value: [],
                        TableField.DISTANCE.value: [],
                    }

                    for result in results:
                        idx = result["index"]
                        data[TableField.ID.value].append(str(idx))
                        data[TableField.CONTENT.value].append(
                            self.dataset.text[idx].data()["value"] if hasattr(self.dataset, "text") else ""
                        )
                        data[TableField.EMBEDDINGS.value].append(self.dataset.embedding[idx].data()["value"].tolist())
                        data[TableField.DISTANCE.value].append(result.get("score", result.get("distance", 0.0)))

                    return pd.DataFrame(data)
                else:
                    # No embeddings in dataset
                    return pd.DataFrame()

            else:
                # Regular query without vector search
                data = {
                    TableField.ID.value: [],
                    TableField.CONTENT.value: [],
                    TableField.EMBEDDINGS.value: [],
                }

                # Apply offset and limit
                start_idx = offset or 0
                end_idx = start_idx + limit

                # Get data from dataset
                if hasattr(self.dataset, "text") and len(self.dataset) > start_idx:
                    actual_end = min(end_idx, len(self.dataset))

                    for idx in range(start_idx, actual_end):
                        data[TableField.ID.value].append(str(idx))
                        data[TableField.CONTENT.value].append(
                            self.dataset.text[idx].data()["value"] if hasattr(self.dataset, "text") else ""
                        )
                        data[TableField.EMBEDDINGS.value].append(
                            self.dataset.embedding[idx].data()["value"].tolist()
                            if hasattr(self.dataset, "embedding")
                            else []
                        )

                return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error in select operation: {e}")
            raise e

    def create_table(self, table_name: str, if_not_exists=True):
        """Create a new Deep Lake dataset."""
        try:
            import deeplake

            dataset_path = self._connection_data["dataset_path"]
            token = self._connection_data.get("token")

            # Check if dataset already exists
            if if_not_exists:
                try:
                    existing_dataset = deeplake.load(dataset_path, token=token)
                    self.dataset = existing_dataset
                    return
                except Exception:
                    pass

            # Create new dataset
            create_kwargs = {
                "path": dataset_path,
                "overwrite": self._create_overwrite,
            }

            if token:
                create_kwargs["token"] = token

            if self._runtime:
                create_kwargs["runtime"] = self._runtime

            self.dataset = deeplake.empty(**create_kwargs)

            # Create tensors for the standard schema
            with self.dataset:
                self.dataset.create_tensor("text", htype="text", sample_compression=self._create_compression)
                self.dataset.create_tensor(
                    "embedding",
                    htype="embedding",
                    dtype=np.float32,
                    max_chunk_size=self._create_max_chunk_size,
                    sample_compression=self._create_compression,
                )
                self.dataset.create_tensor("metadata", htype="json", sample_compression=self._create_compression)
                self.dataset.create_tensor("id", htype="text", sample_compression=self._create_compression)

        except Exception as e:
            logger.error(f"Error creating Deep Lake dataset: {e}")
            raise e

    def insert(self, table_name: str, data: pd.DataFrame, columns: List[str] = None):
        """Insert data into Deep Lake dataset."""
        if not self.dataset:
            raise Exception(f"Dataset {table_name} does not exist")

        try:
            if columns:
                data = data[columns]

            # Process data for insertion
            with self.dataset:
                for _, row in data.iterrows():
                    # Prepare data dict
                    insert_data = {}

                    if TableField.ID.value in row:
                        insert_data["id"] = str(row[TableField.ID.value])
                    else:
                        insert_data["id"] = str(len(self.dataset))

                    if TableField.CONTENT.value in row:
                        insert_data["text"] = str(row[TableField.CONTENT.value])
                    else:
                        insert_data["text"] = ""

                    if TableField.EMBEDDINGS.value in row:
                        embeddings = row[TableField.EMBEDDINGS.value]
                        if isinstance(embeddings, str):
                            embeddings = json.loads(embeddings)
                        if isinstance(embeddings, list):
                            embeddings = np.array(embeddings, dtype=np.float32)
                        insert_data["embedding"] = embeddings
                    else:
                        # Create zero vector if no embedding provided
                        insert_data["embedding"] = np.zeros(self._create_embedding_dim, dtype=np.float32)

                    if TableField.METADATA.value in row:
                        metadata = row[TableField.METADATA.value]
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                        insert_data["metadata"] = metadata
                    else:
                        insert_data["metadata"] = {}

                    # Append to dataset
                    self.dataset.append(insert_data)

        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise e

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        """Delete data from Deep Lake dataset."""
        if not self.dataset:
            raise Exception(f"Dataset {table_name} does not exist")

        if not conditions:
            raise Exception("Delete conditions are required")

        try:
            # Deep Lake doesn't support direct conditional deletion
            # We need to identify indices to delete based on conditions
            indices_to_delete = []

            for condition in conditions:
                if condition.column == TableField.ID.value:
                    if condition.op == FilterOperator.EQUAL:
                        # Find matching ID
                        target_id = str(condition.value)
                        for idx in range(len(self.dataset)):
                            if hasattr(self.dataset, "id") and str(self.dataset.id[idx].data()["value"]) == target_id:
                                indices_to_delete.append(idx)
                    elif condition.op == FilterOperator.IN:
                        # Find matching IDs
                        target_ids = [str(v) for v in condition.value]
                        for idx in range(len(self.dataset)):
                            if hasattr(self.dataset, "id") and str(self.dataset.id[idx].data()["value"]) in target_ids:
                                indices_to_delete.append(idx)

            if indices_to_delete:
                # Delete in reverse order to maintain indices
                for idx in sorted(indices_to_delete, reverse=True):
                    del self.dataset[idx]
            else:
                logger.warning("No matching records found for deletion")

        except Exception as e:
            logger.error(f"Error deleting data: {e}")
            raise e

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get columns/tensors in Deep Lake dataset."""
        try:
            if not self.dataset:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message=f"Dataset {table_name} does not exist",
                )

            # Get tensor names from dataset
            tensor_names = list(self.dataset.tensors.keys())

            # Map to standard schema
            schema_mapping = {
                "id": {"name": TableField.ID.value, "data_type": "string"},
                "text": {"name": TableField.CONTENT.value, "data_type": "string"},
                "embedding": {"name": TableField.EMBEDDINGS.value, "data_type": "list"},
                "metadata": {"name": TableField.METADATA.value, "data_type": "json"},
            }

            schema = []
            for tensor_name in tensor_names:
                if tensor_name in schema_mapping:
                    schema.append(schema_mapping[tensor_name])
                else:
                    schema.append({"name": tensor_name, "data_type": "unknown"})

            if not schema:
                schema = [field for field in self.SCHEMA if field["name"] != TableField.DISTANCE.value]

            data = pd.DataFrame(schema)
            data.columns = ["COLUMN_NAME", "DATA_TYPE"]
            return HandlerResponse(data_frame=data)

        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting columns: {e}",
            )
