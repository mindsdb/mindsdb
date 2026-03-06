from typing import Dict, List, Literal, Optional

import json
import lancedb
import pandas as pd
import pyarrow as pa

from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    DistanceFunction,
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Map distance function names to LanceDB metric types
DISTANCE_METRICS = {
    "l2": "L2",
    "cosine": "cosine",
    "dot": "dot",
}


class LanceDBHandler(VectorStoreHandler, KeywordSearchBase):
    """This handler handles connection and execution of the LanceDB statements.

    Supports:
    - Vector similarity search (L2, cosine, dot product)
    - Full-text keyword search (BM25-based via FTS index)
    - Hybrid search (vector + keyword)
    - Metadata filtering
    """

    name = "lancedb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self._connection_data = kwargs.get("connection_data", {})

        self._client_config = {
            "uri": self._connection_data.get("persist_directory"),
            "api_key": self._connection_data.get("api_key"),
            "region": self._connection_data.get("region"),
            "host_override": self._connection_data.get("host_override"),
        }

        # Remove None values from config
        self._client_config = {k: v for k, v in self._client_config.items() if v is not None}

        # uri is required either for LanceDB Cloud or local
        if not self._client_config.get("uri"):
            raise ValueError("persist_directory is required for LanceDB connection!")

        # uri, api_key and region is required for LanceDB Cloud
        if self._client_config.get("api_key") and not self._client_config.get("region"):
            raise ValueError("region is required for LanceDB Cloud connection!")

        # Configure distance metric
        distance = self._connection_data.get("distance", "l2").lower()
        if distance not in DISTANCE_METRICS:
            raise ValueError(f"Invalid distance metric '{distance}'. Options: {list(DISTANCE_METRICS.keys())}")
        self._distance_metric = DISTANCE_METRICS[distance]

        self._client = None
        self.is_connected = False
        self.connect()

    def _get_client(self):
        if not self._client_config:
            raise ValueError("Client config is not set!")
        return lancedb.connect(**self._client_config)

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def connect(self):
        """Connect to a LanceDB database."""
        if self.is_connected:
            return self._client
        try:
            self._client = self._get_client()
            self.is_connected = True
            return self._client
        except Exception as e:
            logger.error(f"Error connecting to LanceDB client: {e}")
            self.is_connected = False
            raise

    def disconnect(self):
        """Close the database connection."""
        if not self.is_connected:
            return
        self._client = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """Check the connection to the LanceDB database."""
        response = StatusResponse(False)
        need_to_close = not self.is_connected
        try:
            self.connect()
            self._client.table_names()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to LanceDB: {e}")
            response.error_message = str(e)
        finally:
            if response.success and need_to_close:
                self.disconnect()
            if not response.success and self.is_connected:
                self.is_connected = False
        return response

    def _get_lancedb_operator(self, operator: FilterOperator) -> str:
        """Map FilterOperator to LanceDB SQL-like operator."""
        mapping = {
            FilterOperator.EQUAL: "=",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">=",
            FilterOperator.IN: "IN",
            FilterOperator.NOT_IN: "NOT IN",
            FilterOperator.LIKE: "LIKE",
            FilterOperator.NOT_LIKE: "NOT LIKE",
            FilterOperator.IS_NULL: "IS NULL",
            FilterOperator.IS_NOT_NULL: "IS NOT NULL",
        }

        if operator not in mapping:
            raise ValueError(f"Operator {operator} is not supported by LanceDB!")

        return mapping[operator]

    def _translate_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[str]:
        """
        Translate a list of FilterCondition objects to a SQL-like string for LanceDB.

        Handles:
        - Standard columns (id, content)
        - Metadata columns (metadata.field_name or direct field names)

        Example:
            [FilterCondition(column="content", op=EQUAL, value="test")]
            --> "content = 'test'"

            [FilterCondition(column="metadata.category", op=EQUAL, value="docs")]
            --> "metadata['category'] = 'docs'"
        """
        if not conditions:
            return None

        # Filter out vector search conditions
        filtered_conditions = [
            condition
            for condition in conditions
            if condition.column != TableField.SEARCH_VECTOR.value
        ]

        if not filtered_conditions:
            return None

        lancedb_conditions = []
        for condition in filtered_conditions:
            column = condition.column
            op = condition.op
            value = condition.value

            # Handle metadata columns
            if column.startswith(TableField.METADATA.value + "."):
                # Convert metadata.field_name to metadata['field_name']
                field_name = column.split(".", 1)[1]
                column = f"metadata['{field_name}']"
            elif column not in (TableField.ID.value, TableField.CONTENT.value, TableField.EMBEDDINGS.value):
                # Assume it's a metadata field if not a standard column
                column = f"metadata['{column}']"

            # Handle NULL operators
            if op in (FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL):
                lancedb_conditions.append(f"{column} {self._get_lancedb_operator(op)}")
                continue

            # Handle IN/NOT IN operators
            if op in (FilterOperator.IN, FilterOperator.NOT_IN):
                if not isinstance(value, (list, tuple)):
                    value = [value]
                formatted_values = ", ".join(
                    f"'{v}'" if isinstance(v, str) else str(v)
                    for v in value
                )
                lancedb_conditions.append(f"{column} {self._get_lancedb_operator(op)} ({formatted_values})")
                continue

            # Handle LIKE operators
            if op in (FilterOperator.LIKE, FilterOperator.NOT_LIKE):
                lancedb_conditions.append(f"{column} {self._get_lancedb_operator(op)} '{value}'")
                continue

            # Handle standard comparison operators
            if isinstance(value, str):
                formatted_value = f"'{value}'"
            else:
                formatted_value = str(value)

            lancedb_conditions.append(f"{column} {self._get_lancedb_operator(op)} {formatted_value}")

        return " AND ".join(lancedb_conditions) if lancedb_conditions else None

    def _extract_vector_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[List[float]]:
        """Extract the vector search condition from the list of conditions."""
        if not conditions:
            return None

        for condition in conditions:
            if condition.column == TableField.SEARCH_VECTOR.value:
                vector = condition.value
                if isinstance(vector, str):
                    vector = json.loads(vector)
                return vector
        return None

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Select data from a LanceDB table with support for vector search and filtering.

        Args:
            table_name: Name of the table to query
            columns: List of columns to return
            conditions: Filter conditions including optional vector search
            offset: Number of rows to skip
            limit: Maximum number of rows to return

        Returns:
            DataFrame with query results
        """
        self.connect()
        table = self._client.open_table(table_name)

        # Default columns
        if columns is None:
            columns = [TableField.ID.value, TableField.CONTENT.value, TableField.EMBEDDINGS.value, TableField.METADATA.value]

        # Extract vector search condition
        vector = self._extract_vector_condition(conditions)

        # Build filter string for non-vector conditions
        filter_str = self._translate_condition(conditions)

        # Determine which columns to select (excluding distance which is computed)
        select_columns = [c for c in columns if c != TableField.DISTANCE.value]

        if vector is not None:
            # Vector similarity search
            query = table.search(vector, vector_column_name="vector")

            # Set distance metric
            query = query.metric(self._distance_metric)

            # Apply filters
            if filter_str:
                query = query.where(filter_str)

            # Apply limit (required for vector search)
            search_limit = limit if limit else 100
            query = query.limit(search_limit)

            # Select columns
            if select_columns:
                query = query.select(select_columns)

            result = query.to_pandas()

            # Rename _distance to distance
            if "_distance" in result.columns:
                result = result.rename(columns={"_distance": TableField.DISTANCE.value})

            # Apply offset if specified (LanceDB doesn't support offset in search directly)
            if offset:
                result = result.iloc[offset:]

        else:
            # Non-vector query - use LanceDB's native filtering
            if filter_str:
                # Use search with no vector to enable filtering
                query = table.search(None)
                query = query.where(filter_str)
                if limit:
                    query = query.limit(limit + (offset or 0))
                result = query.to_pandas()
            else:
                # Simple table scan
                result = table.to_pandas()

            # Apply offset and limit
            if offset:
                result = result.iloc[offset:]
            if limit:
                result = result.iloc[:limit]

            # Select requested columns
            available_cols = [c for c in select_columns if c in result.columns]
            if available_cols:
                result = result[available_cols]

        # Ensure embeddings are returned as lists for compatibility
        if TableField.EMBEDDINGS.value in result.columns:
            result[TableField.EMBEDDINGS.value] = result[TableField.EMBEDDINGS.value].apply(
                lambda x: list(x) if hasattr(x, '__iter__') and not isinstance(x, str) else x
            )

        # Handle vector column if present
        if "vector" in result.columns and TableField.EMBEDDINGS.value not in result.columns:
            result[TableField.EMBEDDINGS.value] = result["vector"].apply(
                lambda x: list(x) if hasattr(x, '__iter__') and not isinstance(x, str) else x
            )
            result = result.drop(columns=["vector"])

        return result

    def keyword_select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
        keyword_search_args: KeywordSearchArgs = None,
    ) -> pd.DataFrame:
        """
        Perform keyword-based full-text search on a LanceDB table.

        Requires an FTS index to be created on the search column first
        using add_full_text_index().

        Args:
            table_name: Name of the table to search
            columns: List of columns to return
            conditions: Additional filter conditions
            offset: Number of rows to skip
            limit: Maximum number of rows to return
            keyword_search_args: Keyword search parameters (column, query)

        Returns:
            DataFrame with search results ordered by relevance
        """
        self.connect()
        table = self._client.open_table(table_name)

        # Default columns
        if columns is None:
            columns = [TableField.ID.value, TableField.CONTENT.value, TableField.METADATA.value]

        # Build filter string for conditions
        filter_str = self._translate_condition(conditions)

        if keyword_search_args is None:
            raise ValueError("keyword_search_args is required for keyword search")

        # Perform full-text search
        search_column = keyword_search_args.column or TableField.CONTENT.value
        search_query = keyword_search_args.query

        # Use LanceDB's FTS search
        query = table.search(search_query, query_type="fts")

        # Apply filters
        if filter_str:
            query = query.where(filter_str)

        # Apply limit
        search_limit = limit if limit else 100
        if offset:
            search_limit += offset
        query = query.limit(search_limit)

        # Select columns
        select_columns = [c for c in columns if c not in (TableField.DISTANCE.value, TableField.RELEVANCE.value)]
        if select_columns:
            query = query.select(select_columns)

        result = query.to_pandas()

        # Rename score column for consistency
        if "_score" in result.columns:
            result = result.rename(columns={"_score": TableField.RELEVANCE.value})

        # Apply offset
        if offset:
            result = result.iloc[offset:]

        # Ensure embeddings are returned as lists for compatibility
        if TableField.EMBEDDINGS.value in result.columns:
            result[TableField.EMBEDDINGS.value] = result[TableField.EMBEDDINGS.value].apply(
                lambda x: list(x) if hasattr(x, '__iter__') and not isinstance(x, str) else x
            )

        return result

    def insert(self, table_name: str, data: pd.DataFrame):
        """
        Insert data into a LanceDB table.

        Args:
            table_name: Name of the table
            data: DataFrame with columns: id, content, embeddings, metadata
        """
        self.connect()

        # Ensure metadata column exists
        if TableField.METADATA.value not in data.columns:
            data[TableField.METADATA.value] = None

        # Prepare data with required columns
        df = data[[
            TableField.ID.value,
            TableField.CONTENT.value,
            TableField.METADATA.value,
            TableField.EMBEDDINGS.value
        ]].copy()

        # Convert embeddings to proper format for LanceDB
        embeddings = df[TableField.EMBEDDINGS.value].tolist()

        # Determine vector dimensions from first non-null embedding
        vector_dim = None
        for emb in embeddings:
            if emb is not None:
                if isinstance(emb, str):
                    emb = json.loads(emb)
                vector_dim = len(emb)
                break

        if vector_dim is None:
            raise ValueError("No valid embeddings found in data")

        # Convert string embeddings to lists
        def parse_embedding(emb):
            if emb is None:
                return [0.0] * vector_dim
            if isinstance(emb, str):
                return json.loads(emb)
            return list(emb)

        df["vector"] = df[TableField.EMBEDDINGS.value].apply(parse_embedding)

        # Convert metadata to proper format
        def parse_metadata(meta):
            if meta is None or (isinstance(meta, float) and pd.isna(meta)):
                return {}
            if isinstance(meta, str):
                try:
                    return json.loads(meta)
                except json.JSONDecodeError:
                    return {}
            return dict(meta) if meta else {}

        df[TableField.METADATA.value] = df[TableField.METADATA.value].apply(parse_metadata)

        # Ensure ID is string
        df[TableField.ID.value] = df[TableField.ID.value].astype(str)

        try:
            table = self._client.open_table(table_name)
            # Add data to existing table
            table.add(df[[TableField.ID.value, TableField.CONTENT.value, TableField.METADATA.value, "vector"]])
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise

    def update(
        self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None
    ):
        """
        Update data in a LanceDB table.

        LanceDB uses a delete + insert pattern for updates.

        Args:
            table_name: Name of the table
            data: DataFrame with updated data
            key_columns: Columns to use as keys for matching records (default: ['id'])
        """
        self.connect()

        if key_columns is None:
            key_columns = [TableField.ID.value]

        if TableField.ID.value not in data.columns:
            raise ValueError("Data must contain 'id' column for updates")

        table = self._client.open_table(table_name)

        # Delete existing records with matching IDs
        ids_to_update = data[TableField.ID.value].tolist()
        id_list = ", ".join(f"'{id_val}'" for id_val in ids_to_update)
        filter_str = f"{TableField.ID.value} IN ({id_list})"

        try:
            table.delete(filter_str)
        except Exception as e:
            logger.warning(f"Error deleting records during update: {e}")

        # Insert updated records
        self.insert(table_name, data)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        """
        Delete data from a LanceDB table.

        Args:
            table_name: Name of the table
            conditions: Filter conditions for deletion
        """
        self.connect()

        filter_str = self._translate_condition(conditions)
        if filter_str is None:
            raise ValueError("Delete query must have at least one condition!")

        table = self._client.open_table(table_name)
        table.delete(filter_str)

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Create a table with the standard vector store schema.

        Args:
            table_name: Name of the table to create
            if_not_exists: If True, don't error if table exists
        """
        self.connect()

        # Check if table exists
        existing_tables = self._client.table_names()
        if table_name in existing_tables:
            if if_not_exists:
                return
            raise ValueError(f"Table '{table_name}' already exists")

        # Create schema with PyArrow
        # Note: We create with a dummy vector that will be replaced on first insert
        schema = pa.schema([
            pa.field(TableField.ID.value, pa.string()),
            pa.field(TableField.CONTENT.value, pa.string()),
            pa.field(TableField.METADATA.value, pa.struct([])),
            pa.field("vector", pa.list_(pa.float32())),
        ])

        # Create empty table with schema
        empty_table = pa.table({
            TableField.ID.value: pa.array([], type=pa.string()),
            TableField.CONTENT.value: pa.array([], type=pa.string()),
            TableField.METADATA.value: pa.array([], type=pa.struct([])),
            "vector": pa.array([], type=pa.list_(pa.float32())),
        })

        self._client.create_table(table_name, empty_table)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Drop a table from the LanceDB database.

        Args:
            table_name: Name of the table to drop
            if_exists: If True, don't error if table doesn't exist
        """
        self.connect()
        try:
            self._client.drop_table(table_name)
        except Exception as e:
            if not if_exists:
                raise
            logger.debug(f"Table '{table_name}' not found: {e}")

    def get_tables(self) -> HandlerResponse:
        """Get the list of tables in the LanceDB database."""
        self.connect()
        tables = self._client.table_names()
        df = pd.DataFrame(columns=["table_name"], data=tables)
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get column information for a table."""
        self.connect()
        try:
            table = self._client.open_table(table_name)
            schema = table.schema
            columns = []
            for field in schema:
                columns.append({
                    "column_name": field.name,
                    "data_type": str(field.type)
                })
            df = pd.DataFrame(columns)
            return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)
        except Exception as e:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist: {e}",
            )

    def create_index(
        self,
        table_name: str,
        column_name: str = "vector",
        index_type: Literal["ivf_pq", "ivf_hnsw_sq", "ivf_hnsw_pq"] = "ivf_pq",
        metric_type: str = None,
        **kwargs
    ):
        """
        Create a vector index on a LanceDB table.

        Args:
            table_name: Name of the table
            column_name: Name of the vector column (default: "vector")
            index_type: Type of index - "ivf_pq", "ivf_hnsw_sq", or "ivf_hnsw_pq"
            metric_type: Distance metric - "L2", "cosine", or "dot"
            **kwargs: Additional index parameters (num_partitions, num_sub_vectors, etc.)
        """
        self.connect()

        if metric_type is None:
            metric_type = self._distance_metric

        table = self._client.open_table(table_name)

        # Build index configuration
        index_config = {
            "metric": metric_type,
        }

        # Add optional parameters
        if "num_partitions" in kwargs:
            index_config["num_partitions"] = kwargs["num_partitions"]
        if "num_sub_vectors" in kwargs:
            index_config["num_sub_vectors"] = kwargs["num_sub_vectors"]

        # Create the index
        table.create_index(
            column_name,
            index_type=index_type,
            **index_config
        )

        logger.info(f"Created {index_type} index on {table_name}.{column_name}")

    def add_full_text_index(self, table_name: str, column_name: str = "content") -> Response:
        """
        Add a full-text search (FTS) index to a column.

        Args:
            table_name: Name of the table
            column_name: Name of the column to index (default: "content")

        Returns:
            Response indicating success or failure
        """
        self.connect()

        try:
            table = self._client.open_table(table_name)
            table.create_fts_index(column_name)
            logger.info(f"Created FTS index on {table_name}.{column_name}")
            return Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Error creating FTS index: {e}")
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

    def hybrid_search(
        self,
        table_name: str,
        embeddings: List[float],
        query: str = None,
        metadata: Dict[str, str] = None,
        distance_function=DistanceFunction.COSINE_DISTANCE,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Execute a hybrid search combining vector similarity and full-text search.

        Args:
            table_name: Name of the table to search
            embeddings: Query vector for semantic search
            query: Text query for full-text search (optional)
            metadata: Metadata filters to apply (optional)
            distance_function: Distance function for vector search
            **kwargs: Additional parameters (limit, reranker, etc.)

        Returns:
            DataFrame with search results including distance/relevance scores
        """
        self.connect()

        if query is None and metadata is None:
            raise ValueError(
                "Must provide at least one of: query for keyword search, or metadata filters. "
                "For vector-only search, use the select method instead."
            )

        table = self._client.open_table(table_name)
        limit = kwargs.get("limit", 10)

        # Build the hybrid query
        if query is not None:
            # Hybrid search with both vector and text
            search_query = table.search(
                query=embeddings,
                query_type="hybrid"
            ).text(query)
        else:
            # Vector search with metadata filters only
            search_query = table.search(query=embeddings)

        # Apply distance metric
        metric_map = {
            DistanceFunction.COSINE_DISTANCE: "cosine",
            DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE: "L2",
            DistanceFunction.NEGATIVE_DOT_PRODUCT: "dot",
        }
        metric = metric_map.get(distance_function, self._distance_metric)
        search_query = search_query.metric(metric)

        # Apply metadata filters
        if metadata:
            filter_conditions = []
            for key, value in metadata.items():
                if isinstance(value, str):
                    filter_conditions.append(f"metadata['{key}'] = '{value}'")
                else:
                    filter_conditions.append(f"metadata['{key}'] = {value}")
            if filter_conditions:
                search_query = search_query.where(" AND ".join(filter_conditions))

        # Apply limit
        search_query = search_query.limit(limit)

        # Execute and return results
        result = search_query.to_pandas()

        # Rename columns for consistency
        if "_distance" in result.columns:
            result = result.rename(columns={"_distance": TableField.DISTANCE.value})
        if "_relevance_score" in result.columns:
            result = result.rename(columns={"_relevance_score": TableField.RELEVANCE.value})

        return result
