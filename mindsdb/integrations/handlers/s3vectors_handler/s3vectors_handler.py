import os
import ast
import uuid
from typing import List, Optional, Dict, Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import pandas as pd
import numpy as np

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
env = load_dotenv()

DEFAULT_CREATE_TABLE_PARAMS = {
    "dimension": 1536,
    "metric": "cosine",
}
MAX_FETCH_LIMIT = 10000
UPSERT_BATCH_SIZE = 100
MAX_METADATA_KEYS = 10  # S3 Vectors has a hard limit of 10 metadata keys per vector
MAX_GET_VECTORS_KEYS = 100  # S3 Vectors API limit for GetVectors operation


class S3VectorsHandler(VectorStoreHandler):
    """This handler handles connection and execution of AWS S3 Vectors statements."""

    name = "s3vectors"

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.session = None
        self.is_connected = False
        self.vector_bucket = connection_data.get("vector_bucket")

        if not self.vector_bucket:
            raise ValueError("Required parameter 'vector_bucket' must be provided.")

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def get_metadata_limits(self) -> Optional[Dict[str, int]]:
        """
        S3 Vectors has a hard limit of 10 metadata keys per vector.

        Returns:
            Dictionary with 'max_keys' set to 10
        """
        return {'max_keys': MAX_METADATA_KEYS}

    def _get_s3vectors_operator(self, operator: FilterOperator) -> str:
        """Convert FilterOperator to an operator that S3 Vectors query language can understand"""
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
            raise Exception(f"Operator {operator} is not supported by S3 Vectors!")
        return mapping[operator]

    def _translate_metadata_condition(self, conditions: List[FilterCondition]) -> Optional[Dict]:
        """
        Translate a list of FilterCondition objects to a dict that can be used by S3 Vectors.

        Example:
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
        if conditions is None:
            return None

        # Filter only metadata conditions
        metadata_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        if len(metadata_conditions) == 0:
            return None

        # Translate each metadata condition into a dict
        s3vectors_conditions = []
        for condition in metadata_conditions:
            metadata_key = condition.column.split(".")[-1]
            s3vectors_conditions.append(
                {
                    metadata_key: {
                        self._get_s3vectors_operator(condition.op): condition.value
                    }
                }
            )

        # Combine all metadata conditions into a single dict
        metadata_condition = (
            {"$and": s3vectors_conditions}
            if len(s3vectors_conditions) > 1
            else s3vectors_conditions[0]
        )
        return metadata_condition

    def connect(self):
        """Connect to AWS S3 Vectors service."""
        if self.is_connected is True:
            return self.connection

        try:
            session_kwargs = {}

            # Authentication priority: explicit credentials → IAM role → AWS profile
            has_credentials = (
                "aws_access_key_id" in self.connection_data
                and "aws_secret_access_key" in self.connection_data
            )

            if has_credentials:
                # Use explicit credentials
                session_kwargs["aws_access_key_id"] = self.connection_data["aws_access_key_id"]
                session_kwargs["aws_secret_access_key"] = self.connection_data["aws_secret_access_key"]

                # Optional session token
                if "aws_session_token" in self.connection_data:
                    session_kwargs["aws_session_token"] = self.connection_data["aws_session_token"]

            # Optional region (works with all auth methods)
            if "region_name" in self.connection_data:
                session_kwargs["region_name"] = self.connection_data["region_name"]

            # Create session and client
            
            if os.getenv("AWS_PROFILE"):
                self.session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))
            else:
                self.session = boto3.Session(**session_kwargs)
                
            self.connection = self.session.client("s3vectors")
            # Verify connection by listing buckets
            
            self.connection.list_indexes(vectorBucketName=self.connection_data["vector_bucket"])

            self.is_connected = True
            return self.connection

        except Exception as e:
            logger.error(f"Error connecting to S3 Vectors client: {e}")
            self.is_connected = False
            raise

    def disconnect(self):
        """Close the S3 Vectors connection."""
        if self.is_connected is False:
            return

        if self.connection:
            self.connection.close()

        self.connection = None
        self.session = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """Check the connection to S3 Vectors."""
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            # Verify we can access the vector bucket
            connection.list_indexes(vectorBucketName=self.vector_bucket)
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to S3 Vectors: {e}")
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def get_tables(self) -> HandlerResponse:
        """Get the list of indexes in the S3 Vectors bucket."""
        connection = self.connect()

        try:
            response = connection.list_indexes(vectorBucketName=self.vector_bucket)
            indexes = response.get("indexes", [])

            df = pd.DataFrame(
                columns=["table_name"],
                data=[index["indexName"] for index in indexes],
            )
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)
        except Exception as e:
            logger.error(f"Error listing indexes: {e}")
            raise Exception(f"Error listing indexes from bucket '{self.vector_bucket}': {e}")

    def create_table(self, table_name: str, if_not_exists=True):
        """Create a vector index with the given name in the S3 Vectors bucket."""
        connection = self.connect()

        # Prepare create parameters
        create_params = {}
        for key, val in DEFAULT_CREATE_TABLE_PARAMS.items():
            if key in self.connection_data:
                create_params[key] = self.connection_data[key]
            else:
                create_params[key] = val

        try:
            connection.create_index(
                vectorBucketName=self.vector_bucket,
                indexName=table_name,
                dataType="float32",
                dimension=create_params["dimension"],
                distanceMetric=create_params["metric"],
            )
            logger.info(f"Created index '{table_name}' in bucket '{self.vector_bucket}'")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "IndexAlreadyExists" and if_not_exists:
                logger.info(f"Index '{table_name}' already exists")
                return
            raise Exception(f"Error creating index '{table_name}': {e}")

    def insert(self, table_name: str, data: pd.DataFrame):
        """Insert data into S3 Vectors index."""
        connection = self.connect()

        logger.info(f"Inserting {len(data)} vectors into '{table_name}'")
        logger.debug(f"Input columns: {data.columns.tolist()}")

        # Rename columns to match expected format
        data = data.copy()
        data.rename(columns={
            TableField.ID.value: "id",
            TableField.EMBEDDINGS.value: "values"},
            inplace=True)

        # Generate UUIDs for missing IDs
        if "id" not in data.columns or data["id"].isnull().any():
            logger.info("Generating UUIDs for missing IDs")
            # Create id column if it doesn't exist
            if "id" not in data.columns:
                data["id"] = None
            # Generate UUID for each row with missing ID
            data["id"] = data["id"].apply(
                lambda x: str(uuid.uuid4()) if pd.isna(x) or x is None or str(x).strip() == "" else str(x)
            )
            logger.debug(f"Generated {data['id'].isnull().sum()} UUIDs")

        columns = ["id", "values"]

        # Handle content - store it in metadata
        if TableField.CONTENT.value in data.columns:
            # If metadata doesn't exist, create it
            if TableField.METADATA.value not in data.columns:
                data[TableField.METADATA.value] = [{}] * len(data)

            # Store content in metadata with key '_content'
            data[TableField.METADATA.value] = data.apply(
                lambda row: {
                    **({} if row[TableField.METADATA.value] is None or
                       (isinstance(row[TableField.METADATA.value], float) and np.isnan(row[TableField.METADATA.value]))
                       else row[TableField.METADATA.value]),
                    '_content': row[TableField.CONTENT.value]
                } if pd.notna(row.get(TableField.CONTENT.value)) else
                  ({} if row[TableField.METADATA.value] is None or
                   (isinstance(row[TableField.METADATA.value], float) and np.isnan(row[TableField.METADATA.value]))
                   else row[TableField.METADATA.value]),
                axis=1
            )

        # Handle metadata
        if TableField.METADATA.value in data.columns:
            data.rename(columns={TableField.METADATA.value: "metadata"}, inplace=True)
            # Fill None and NaN values with empty dict
            if data['metadata'].isnull().any():
                data['metadata'] = data['metadata'].apply(
                    lambda x: {} if x is None or (isinstance(x, float) and np.isnan(x)) else x
                )
            columns.append("metadata")

        data = data[columns]

        # Convert embeddings to lists if they are strings
        data["values"] = data["values"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        # Capture IDs before insertion to return them later
        inserted_ids = data["id"].tolist()

        # Insert in batches
        for chunk in (data[pos:pos + UPSERT_BATCH_SIZE] for pos in range(0, len(data), UPSERT_BATCH_SIZE)):
            vectors = []
            for _, row in chunk.iterrows():
                vector_entry = {
                    "key": str(row["id"]),
                    "data": {"float32": row["values"]}
                }
                if "metadata" in row and row["metadata"]:
                    vector_entry["metadata"] = row["metadata"]
                vectors.append(vector_entry)

            try:
                logger.debug(f"Inserting batch of {len(vectors)} vectors into '{table_name}'")
                connection.put_vectors(
                    vectorBucketName=self.vector_bucket,
                    indexName=table_name,
                    vectors=vectors
                )
                logger.debug(f"Successfully inserted batch of {len(vectors)} vectors")
            except Exception as e:
                logger.error(f"Error inserting vectors into '{table_name}': {e}")
                logger.error(f"First vector in batch: {vectors[0] if vectors else 'No vectors'}")
                raise Exception(f"Error inserting vectors into S3Vectors index '{table_name}': {e}")

        # Return DataFrame with inserted IDs so users can track and manage them
        return pd.DataFrame({TableField.ID.value: inserted_ids})

    def drop_table(self, table_name: str, if_exists=True):
        """Delete an index from the S3 Vectors bucket."""
        connection = self.connect()

        try:
            connection.delete_index(
                vectorBucketName=self.vector_bucket,
                indexName=table_name
            )
            logger.info(f"Deleted index '{table_name}' from bucket '{self.vector_bucket}'")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchIndex" and if_exists:
                logger.info(f"Index '{table_name}' does not exist")
                return
            raise Exception(f"Error deleting index '{table_name}': {e}")

    def delete(self, table_name: str, conditions: List[FilterCondition] = None):
        """Delete records in S3 Vectors index based on IDs or metadata conditions."""
        if conditions is None or len(conditions) == 0:
            raise Exception("Delete query must have at least one condition!")

        connection = self.connect()

        # Extract ID filters
        ids = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ]

        # Extract metadata filters
        filters = self._translate_metadata_condition(conditions)

        if not ids and filters is None:
            raise Exception("Delete query must have either id condition or metadata condition!")

        try:
            if ids:
                # Delete by IDs
                connection.delete_vectors(
                    vectorBucketName=self.vector_bucket,
                    indexName=table_name,
                    keys=[str(id_val) for id_val in ids]
                )
            else:
                # Delete by metadata filter
                # Note: This may require query + delete approach
                # Implementation depends on S3 Vectors API support
                logger.warning("Metadata-based deletion may require additional implementation")
                raise NotImplementedError("Deletion by metadata filter not yet implemented")
        except Exception as e:
            logger.error(f"Error deleting from '{table_name}': {e}")
            raise Exception(f"Error deleting vectors: {e}")

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ):
        """Run query on S3 Vectors index and get results."""
        connection = self.connect()

        query = {
            "include_values": True,
            "include_metadata": True
        }

        # Check for metadata filter
        metadata_filters = self._translate_metadata_condition(conditions)
        if metadata_filters is not None:
            query["filter"] = metadata_filters

        # Check for vector and id filters
        vector_filters = []
        id_filters = []

        if conditions:
            for condition in conditions:
                if condition.column == TableField.SEARCH_VECTOR.value:
                    vector_filters.append(condition.value)
                elif condition.column == TableField.ID.value:
                    # Handle both single values and IN operator with lists
                    if condition.op == FilterOperator.IN:
                        # IN operator: value is a list of IDs
                        id_filters.extend(condition.value if isinstance(condition.value, list) else [condition.value])
                    else:
                        # EQUAL operator: single ID value
                        id_filters.append(condition.value)

        # Execute query based on filter type
        if vector_filters:
            # Similarity search
            if len(vector_filters) > 1:
                raise Exception("You cannot have multiple search_vectors in query")

            query_vector = vector_filters[0]

            # Handle vector as string representation
            if isinstance(query_vector, list) and isinstance(query_vector[0], str):
                if len(query_vector) > 1:
                    raise Exception("You cannot have multiple search_vectors in query")
                try:
                    query_vector = ast.literal_eval(query_vector[0])
                except Exception as e:
                    raise Exception(f"Cannot parse the search vector '{query_vector}' into a list: {e}")

            # Set limit
            top_k = limit if limit is not None else MAX_FETCH_LIMIT

            # Execute similarity search
            try:
                query_params = {
                    "vectorBucketName": self.vector_bucket,
                    "indexName": table_name,
                    "queryVector": {"float32": query_vector},
                    "topK": top_k,
                    "returnMetadata": True,
                    "returnDistance": True,
                }

                if metadata_filters:
                    query_params["filter"] = metadata_filters
                result = connection.query_vectors(**query_params)
            except Exception as e:
                raise Exception(f"Error running SELECT query on '{table_name}': {e}")

            # Parse results
            matches = result.get("vectors", [])
            results_data = []
            for match in matches:
                metadata = match.get("metadata", {})
                # Extract content from metadata if it exists
                content = metadata.get("_content", "")

                results_data.append({
                    TableField.ID.value: match.get("key"),
                    TableField.CONTENT.value: content,
                    TableField.EMBEDDINGS.value: match.get("vector", []),
                    TableField.METADATA.value: metadata,
                    "distance": match.get("distance", 0.0),
                })

            results_df = pd.DataFrame(results_data) if results_data else pd.DataFrame(columns=[
                TableField.ID.value,
                TableField.CONTENT.value,
                TableField.EMBEDDINGS.value,
                TableField.METADATA.value,
                "distance"
            ])

        elif id_filters:
            # Get by IDs (supports single or multiple IDs)
            # AWS S3 Vectors API has a limit of 100 keys per GetVectors call, so batch requests
            results_data = []

            # Convert all IDs to strings
            id_strings = [str(id_val) for id_val in id_filters]

            # Batch the requests in chunks of MAX_GET_VECTORS_KEYS
            for i in range(0, len(id_strings), MAX_GET_VECTORS_KEYS):
                batch_ids = id_strings[i:i + MAX_GET_VECTORS_KEYS]

                try:
                    result = connection.get_vectors(
                        vectorBucketName=self.vector_bucket,
                        indexName=table_name,
                        keys=batch_ids
                    )

                    # Parse results
                    vectors = result.get("Vectors", [])
                    for vector in vectors:
                        metadata = vector.get("Metadata", {})
                        # Extract content from metadata if it exists
                        content = metadata.get("_content", "")

                        results_data.append({
                            TableField.ID.value: vector.get("Key"),
                            TableField.CONTENT.value: content,
                            TableField.EMBEDDINGS.value: vector.get("Vector", []),
                            TableField.METADATA.value: metadata,
                        })

                except Exception as e:
                    logger.warning(f"Error querying vectors by ID batch from '{table_name}': {e}")
                    # Continue with next batch instead of failing completely
                    continue

            results_df = pd.DataFrame(results_data) if results_data else pd.DataFrame(columns=[
                TableField.ID.value,
                TableField.CONTENT.value,
                TableField.EMBEDDINGS.value,
                TableField.METADATA.value,
            ])
            if "distance" not in results_df.columns:
                results_df["distance"] = None
        else:
            raise Exception("You must provide either a search_vector or an ID in the query")

        # Ensure content column exists (should be populated from metadata now)
        if TableField.CONTENT.value not in results_df.columns:
            results_df[TableField.CONTENT.value] = ""

        # Select requested columns
        if columns:
            # Ensure all requested columns exist, fill missing ones with appropriate defaults
            for col in columns:
                if col not in results_df.columns:
                    if col == TableField.CONTENT.value:
                        results_df[col] = ""
                    elif col == TableField.DISTANCE.value:
                        results_df[col] = None
                    elif col == TableField.EMBEDDINGS.value:
                        results_df[col] = None
                    elif col == TableField.METADATA.value:
                        results_df[col] = {}
                    elif col == TableField.ID.value:
                        results_df[col] = None
                    else:
                        results_df[col] = None

            results_df = results_df[columns]

        return results_df

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get columns for the table (returns standard vector DB schema)."""
        return super().get_columns(table_name)
