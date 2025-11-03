from typing import Text, Dict, Optional, List, Any
import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError,
    AuthenticationException,
    TransportError,
    RequestError,
)

# ApiError is only available in Elasticsearch 8+
try:
    from elasticsearch.exceptions import ApiError
except ImportError:
    ApiError = Exception  # Fallback for ES 7.x compatibility

# ESDialect: SQLAlchemy dialect for Elasticsearch, enables SQL query rendering
from es.elastic.sqlalchemy import ESDialect
from pandas import DataFrame
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ElasticsearchHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Elasticsearch
    using a SQL-first architecture with automatic fallback capabilities.

    Features:
        - SQL-first query execution with automatic Search API fallback
        - Intelligent array field detection and JSON conversion
        - SSL/TLS security configuration support
        - Memory-efficient large dataset handling with pagination
        - Comprehensive error handling and recovery mechanisms
    """

    name = "elasticsearch"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the Elasticsearch handler with SQL-first query execution.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Elasticsearch cluster.
                Should include hosts/cloud_id and authentication parameters.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False
        self._array_fields_cache: Dict[str, List[str]] = {}

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> Elasticsearch:
        """
        Establishes a connection to the Elasticsearch host with security configuration support.

        This method supports both on-premises and cloud Elasticsearch deployments with
        SSL/TLS configuration and authentication options.

        Returns:
            elasticsearch.Elasticsearch: A connection object to the Elasticsearch host.

        Raises:
            ValueError: If the expected connection parameters are not provided.
            ConnectionError: If unable to establish connection to Elasticsearch.
            AuthenticationException: If authentication fails.
        """
        if self.is_connected:
            return self.connection

        # Validate required parameters
        if not self.connection_data.get("hosts") and not self.connection_data.get("cloud_id"):
            raise ValueError("Either 'hosts' or 'cloud_id' parameter must be provided")

        config = {}

        # Connection parameters
        if "hosts" in self.connection_data:
            hosts_str = self.connection_data["hosts"]
            hosts = hosts_str.split(",")

            # Validate host:port format
            for host in hosts:
                host = host.strip()
                if ":" not in host:
                    raise ValueError(
                        f"Invalid host format '{host}'. Expected format: 'host:port' (e.g., 'localhost:9200')"
                    )
                # Additional validation: check port is numeric
                try:
                    host_part, port_part = host.rsplit(":", 1)
                    int(port_part)  # Validate port is numeric
                except ValueError:
                    raise ValueError(f"Invalid port in host '{host}'. Port must be numeric")

            config["hosts"] = hosts
        if "cloud_id" in self.connection_data:
            config["cloud_id"] = self.connection_data["cloud_id"]

        # Authentication - API key takes precedence
        if "api_key" in self.connection_data:
            config["api_key"] = self.connection_data["api_key"]
            # Skip user/password if API key is provided
        else:
            # Only check user/password if API key is not provided
            user = self.connection_data.get("user")
            password = self.connection_data.get("password")
            if user and password:
                config["http_auth"] = (user, password)
            elif user or password:
                raise ValueError("Both 'user' and 'password' must be provided together")

        # SSL/TLS configuration (secure by default)
        config["verify_certs"] = self.connection_data.get("verify_certs", True)
        if "ca_certs" in self.connection_data:
            config["ca_certs"] = self.connection_data["ca_certs"]
        if "client_cert" in self.connection_data:
            config["client_cert"] = self.connection_data["client_cert"]
        if "client_key" in self.connection_data:
            config["client_key"] = self.connection_data["client_key"]
        if "timeout" in self.connection_data:
            config["timeout"] = self.connection_data["timeout"]

        try:
            self.connection = Elasticsearch(**config)
            self.is_connected = True
            return self.connection
        except (ConnectionError, AuthenticationException) as e:
            logger.error(f"Connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}")
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the Elasticsearch host if it's currently open.
        """
        if not self.is_connected:
            return
        try:
            self.connection.close()
        finally:
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Elasticsearch host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()
            # Simple test query
            connection.sql.query(body={"query": "SELECT 1"})
            response.success = True
        except Exception as error:
            logger.error(f"Connection check failed: {error}")
            response.error_message = str(error)
            if self.is_connected:
                self.is_connected = False

        if response.success and need_to_close:
            self.disconnect()

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Elasticsearch host using SQL-first approach.

        This method uses a dual-strategy approach:
        1. Primary: Uses Elasticsearch SQL API for performance and compatibility
        2. Fallback: Automatically switches to Search API for array-containing indexes
        3. Handles pagination and large result sets

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        logger.debug(f"Executing query: {query[:100]}...")
        need_to_close = not self.is_connected
        connection = self.connect()

        try:
            # Primary: Try SQL API first (standard approach)
            response = connection.sql.query(body={"query": query})
            records = response["rows"]
            columns = response["columns"]

            # Handle pagination for large result sets with safety limit
            max_pages = 100  # Prevent infinite pagination
            for _ in range(max_pages):
                if not response.get("cursor"):
                    break
                response = connection.sql.query(body={"query": query, "cursor": response["cursor"]})
                if not response["rows"]:
                    break
                records.extend(response["rows"])

            column_names = [col["name"] for col in columns]
            if not records:
                records = [[None] * len(column_names)]

            return Response(RESPONSE_TYPE.TABLE, data_frame=DataFrame(records, columns=column_names))

        except (TransportError, RequestError, ApiError) as e:
            error_msg = str(e).lower()

            # Intelligent fallback: Check if error is array-related
            if any(keyword in error_msg for keyword in ["array", "nested", "object"]):
                logger.debug(f"SQL API failed with array-related error, using Search API fallback: {e}")
                try:
                    return self._search_api_fallback(query)
                except Exception as fallback_error:
                    logger.error(f"Search API fallback also failed: {fallback_error}")
                    return Response(
                        RESPONSE_TYPE.ERROR, error_message=f"Both SQL and Search APIs failed: {fallback_error}"
                    )

            # Handle other SQL API errors
            logger.error(f"SQL API error: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        except Exception as e:
            logger.error(f"Unexpected query error: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        finally:
            if need_to_close:
                self.disconnect()

    def _search_api_fallback(self, query: str) -> Response:
        """
        Search API fallback for array-containing indexes.

        This method is automatically invoked when SQL API encounters array fields,
        providing seamless query execution with proper array handling.

        Args:
            query (str): Original SQL query that failed with SQL API

        Returns:
            Response: Search results converted to tabular format with arrays as JSON strings
        """
        # Simple query parsing (only what's needed for Search API)
        index_name = self._extract_table_name(query)
        if not index_name:
            raise ValueError("Could not determine index name from query")

        # Extract LIMIT from query if present
        limit = self._extract_limit(query)
        if limit is None:
            limit = 10000  # Default maximum documents to fetch to prevent memory issues

        # Execute search with pagination
        scroll_id = None
        try:
            batch_size = min(1000, limit)  # Use smaller batch size if limit is small
            search_body = {
                "size": batch_size,
                "query": {"match_all": {}},
            }

            response = self.connection.search(index=index_name, body=search_body, scroll="5m")

            records = []
            all_columns = set()
            scroll_id = response.get("_scroll_id")
            processed_count = 0

            # Process results in batches with explicit limit
            max_batches = (limit // batch_size) + 1  # Calculate max batches needed
            for _ in range(max_batches):
                hits = response.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    if processed_count >= limit:
                        break

                    doc = hit.get("_source", {})
                    if doc:
                        converted_doc = self._convert_arrays_to_strings(doc)
                        flattened_doc = self._flatten_document(converted_doc)
                        if flattened_doc:
                            records.append(flattened_doc)
                            all_columns.update(flattened_doc.keys())
                            processed_count += 1

                # Get next batch if we haven't reached the limit
                if not scroll_id or processed_count >= limit:
                    break
                try:
                    response = self.connection.scroll(scroll_id=scroll_id, scroll="5m")
                except Exception:
                    break

            # Normalize records
            columns = sorted(all_columns) if all_columns else ["no_data"]
            normalized_records = []

            for record in records:
                normalized_records.append([record.get(col) for col in columns])

            if not normalized_records:
                normalized_records = [[None] * len(columns)]

            return Response(RESPONSE_TYPE.TABLE, data_frame=DataFrame(normalized_records, columns=columns))

        except Exception as e:
            raise Exception(f"Search API execution failed: {e}")
        finally:
            # Clean up scroll - ensures cleanup even if exceptions occur
            if scroll_id:
                try:
                    self.connection.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

    def _extract_table_name(self, query: str) -> Optional[str]:
        """
        Extracts the table/index name from a SQL query.

        Args:
            query (str): SQL query string

        Returns:
            Optional[str]: The extracted table name, or None if not found
        """
        import re

        match = re.search(r'FROM\s+([`"]?)([^`"\s]+)\1', query, re.IGNORECASE)
        return match.group(2) if match else None

    def _extract_limit(self, query: str) -> Optional[int]:
        """
        Extracts the LIMIT value from a SQL query.

        Args:
            query (str): SQL query string

        Returns:
            Optional[int]: The extracted limit value, or None if not found
        """
        import re

        match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    def _detect_array_fields(self, index_name: str) -> List[str]:
        """
        Detects array fields in the specified index with caching.

        Args:
            index_name (str): The name of the index to analyze

        Returns:
            List[str]: List of field paths that contain arrays
        """
        if index_name in self._array_fields_cache:
            return self._array_fields_cache[index_name]

        array_fields = []
        try:
            response = self.connection.search(
                index=index_name, body={"size": 5, "query": {"match_all": {}}}, _source=True
            )

            for hit in response.get("hits", {}).get("hits", []):
                doc = hit.get("_source", {})
                array_fields.extend(self._find_arrays_in_doc(doc))

            array_fields = list(set(array_fields))

            # Only cache non-empty results to prevent false negatives
            if array_fields:
                self._array_fields_cache[index_name] = array_fields

        except Exception as e:
            logger.error(f"Array field detection failed for {index_name}: {e}")

        return array_fields

    def _find_arrays_in_doc(self, doc: Any, prefix: str = "") -> List[str]:
        """
        Recursively finds array fields in a document.

        Args:
            doc (Any): The document to analyze
            prefix (str): Current field path prefix for nested fields

        Returns:
            List[str]: List of field paths containing arrays
        """
        arrays = []
        if isinstance(doc, dict):
            for key, value in doc.items():
                field_path = f"{prefix}.{key}" if prefix else key
                if isinstance(value, list):
                    arrays.append(field_path)
                elif isinstance(value, dict):
                    arrays.extend(self._find_arrays_in_doc(value, field_path))
        return arrays

    def _convert_arrays_to_strings(self, obj: Any) -> Any:
        """
        Converts arrays to JSON strings for SQL compatibility.

        Args:
            obj (Any): Object that may contain arrays

        Returns:
            Any: Object with arrays converted to JSON strings
        """
        if isinstance(obj, list):
            try:
                return json.dumps(obj, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                return str(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_arrays_to_strings(v) for k, v in obj.items()}
        return obj

    def _flatten_document(self, doc: Dict, prefix: str = "", max_depth: int = 10, _depth: int = 0) -> Dict:
        """
        Flattens nested documents with depth protection to prevent stack overflow.

        Args:
            doc (Dict): Document to flatten
            prefix (str): Field path prefix for nested fields
            max_depth (int): Maximum recursion depth to prevent stack overflow
            _depth (int): Current recursion depth (internal use)

        Returns:
            Dict: Flattened document with dot-notation field names
        """
        if not isinstance(doc, dict) or _depth >= max_depth:
            return {prefix or "value": str(doc)}

        flattened = {}
        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flattened.update(self._flatten_document(value, field_path, max_depth, _depth + 1))
            else:
                flattened[field_path] = value

        return flattened

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Elasticsearch host.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        try:
            renderer = SqlalchemyRender(ESDialect)
            query_str = renderer.get_string(query, with_failback=True)
            logger.debug(f"Executing AST query as SQL: {query_str}")
            return self.native_query(query_str)
        except Exception as e:
            logger.error(f"AST query execution failed: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables (indexes) in the Elasticsearch host.

        Returns:
            Response: A response object containing a list of tables (indexes) in the Elasticsearch host.
                System indices (starting with '.') are filtered out.
        """
        query = "SHOW TABLES"
        result = self.native_query(query)

        if result.type == RESPONSE_TYPE.TABLE:
            df = result.data_frame
            # Filter out system indexes (starting with .)
            df = df[~df["name"].str.startswith(".")]
            df = df.drop(["catalog", "kind"], axis=1, errors="ignore")
            result.data_frame = df.rename(columns={"name": "table_name", "type": "table_type"})

        return result

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column (field) details for a specified table (index) in the Elasticsearch host.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details.

        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string")

        query = f"DESCRIBE {table_name}"
        result = self.native_query(query)

        if result.type == RESPONSE_TYPE.TABLE:
            df = result.data_frame
            df = df.drop("mapping", axis=1, errors="ignore")
            result.data_frame = df.rename(columns={"column": "COLUMN_NAME", "type": "DATA_TYPE"})

        return result

    def get_column_statistics(self, table_name: str, column_name: Optional[str] = None) -> Response:
        """
        Retrieves statistics for columns in the specified Elasticsearch index.

        This method uses Elasticsearch aggregations to efficiently gather statistics in a single query:
        - Numeric fields: min, max, avg (via stats aggregation)
        - Keyword fields: distinct count (cardinality)
        - Text fields: distinct count (cardinality on .keyword multi-field)
        - Date fields: min, max (via stats aggregation, as timestamps)
        - All fields: null count (missing values)
        - Object/nested fields: excluded from aggregations, null count only
        - Nested/array fields: treated as text (cardinality on JSON string representation)

        Implementation Details:
        - Text fields use the .keyword multi-field suffix for aggregations
        - Object and nested types are skipped for cardinality (not aggregatable)
        - If aggregations fail (e.g., text field without .keyword), returns schema with NULL values
        - All statistics gathered in a single Elasticsearch search query for performance

        Args:
            table_name (str): The name of the index to analyze.
            column_name (Optional[str]): Specific column name. If None, returns statistics for all columns.

        Returns:
            Response: DataFrame with columns:
                - column_name: Field name
                - data_type: Elasticsearch field type
                - null_count: Number of documents missing this field (0 if aggregation failed)
                - distinct_count: Approximate count of unique values (0 if not aggregatable)
                - min: Minimum value (numeric/date fields, None otherwise)
                - max: Maximum value (numeric/date fields, None otherwise)
                - avg: Average value (numeric fields only, None otherwise)

        Raises:
            ValueError: If table_name is invalid or column_name not found in index.

        Example:
            >>> handler.get_column_statistics('kibana_sample_data_flights')
            >>> handler.get_column_statistics('products', 'price')
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string")

        logger.debug(f"Getting column statistics for {table_name}, column: {column_name}")
        need_to_close = not self.is_connected
        connection = self.connect()

        try:
            # Step 1: Get index mapping to determine field types
            mapping_response = connection.indices.get_mapping(index=table_name)

            # Extract field mappings (handle both single and multi-index responses)
            if table_name in mapping_response:
                properties = mapping_response[table_name].get("mappings", {}).get("properties", {})
            else:
                # For wildcard or first index in response
                first_index = list(mapping_response.keys())[0]
                properties = mapping_response[first_index].get("mappings", {}).get("properties", {})

            if not properties:
                logger.warning(f"No properties found for index {table_name}")
                return Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=DataFrame(
                        columns=["column_name", "data_type", "null_count", "distinct_count", "min", "max", "avg"]
                    ),
                )

            # Step 2: Flatten nested field mappings and filter by column_name if provided
            fields_to_analyze = {}
            self._extract_fields_from_mapping(properties, fields_to_analyze, prefix="")

            if column_name:
                if column_name not in fields_to_analyze:
                    raise ValueError(f"Column '{column_name}' not found in index '{table_name}'")
                fields_to_analyze = {column_name: fields_to_analyze[column_name]}

            # Step 3: Build comprehensive aggregation query
            aggs = {}
            for field_name, field_info in fields_to_analyze.items():
                field_type = field_info.get("type", "object")
                safe_field_name = field_name.replace(".", "_")

                # Skip object/nested types - they don't support aggregations
                if field_type in ["object", "nested"]:
                    continue

                # Determine aggregation field (text fields need .keyword suffix)
                agg_field = field_name
                if field_type == "text":
                    # Check if .keyword multi-field exists in mapping
                    multi_fields = field_info.get("fields", {})
                    if "keyword" in multi_fields:
                        # Text field has .keyword multi-field for aggregations
                        agg_field = f"{field_name}.keyword"
                    else:
                        # Text field without .keyword - skip this field
                        # (fielddata would need to be enabled, which is not recommended for text fields)
                        logger.debug(f"Text field '{field_name}' has no .keyword multi-field, skipping")
                        continue

                # Cardinality aggregation for distinct count
                aggs[f"{safe_field_name}_cardinality"] = {
                    "cardinality": {
                        "field": agg_field,
                        "precision_threshold": 3000,  # Improves performance on large datasets
                    }
                }

                # Missing aggregation for null count
                aggs[f"{safe_field_name}_missing"] = {"missing": {"field": field_name}}

                # Stats aggregation for numeric and date fields
                if field_type in [
                    "long",
                    "integer",
                    "short",
                    "byte",
                    "double",
                    "float",
                    "half_float",
                    "scaled_float",
                    "date",
                ]:
                    aggs[f"{safe_field_name}_stats"] = {"stats": {"field": field_name}}

            # Step 4: Execute single aggregation query for all statistics
            search_body = {
                "size": 0,  # We only need aggregations, not documents
                "aggs": aggs,
            }

            logger.debug(f"Executing aggregation query with {len(aggs)} aggregations")

            # Execute aggregation query with error handling for field-specific failures
            try:
                agg_response = connection.search(index=table_name, body=search_body)
            except Exception as search_error:
                # If aggregation fails (e.g., text field without .keyword), log and retry without problematic aggs
                error_msg = str(search_error).lower()
                if "fielddata" in error_msg or "keyword" in error_msg or "text" in error_msg:
                    logger.warning(f"Aggregation failed, possibly due to text field without fielddata: {search_error}")
                    # Return basic statistics without aggregations
                    stats_data = []
                    for field_name, field_info in fields_to_analyze.items():
                        stats_data.append(
                            {
                                "column_name": field_name,
                                "data_type": field_info.get("type", "object"),
                                "null_count": None,
                                "distinct_count": None,
                                "min": None,
                                "max": None,
                                "avg": None,
                            }
                        )
                    return Response(RESPONSE_TYPE.TABLE, data_frame=DataFrame(stats_data))
                else:
                    raise

            # Step 5: Parse aggregation results into statistics
            stats_data = []
            for field_name, field_info in fields_to_analyze.items():
                field_type = field_info.get("type", "object")
                safe_field_name = field_name.replace(".", "_")

                aggregations = agg_response.get("aggregations", {})

                # Extract cardinality (distinct count)
                cardinality_key = f"{safe_field_name}_cardinality"
                cardinality_result = aggregations.get(cardinality_key, {})
                distinct_count = int(cardinality_result.get("value", 0)) if cardinality_result else 0

                # Extract missing count (null count)
                missing_key = f"{safe_field_name}_missing"
                missing_result = aggregations.get(missing_key, {})
                null_count = missing_result.get("doc_count", 0) if missing_result else 0

                # Extract stats for numeric/date fields
                stats_key = f"{safe_field_name}_stats"
                stats = aggregations.get(stats_key, {})

                min_val = stats.get("min") if stats else None
                max_val = stats.get("max") if stats else None
                avg_val = stats.get("avg") if stats else None

                stats_data.append(
                    {
                        "column_name": field_name,
                        "data_type": field_type,
                        "null_count": null_count,
                        "distinct_count": distinct_count,
                        "min": min_val,
                        "max": max_val,
                        "avg": avg_val,
                    }
                )

            result_df = DataFrame(stats_data)
            logger.debug(f"Retrieved statistics for {len(stats_data)} fields")

            return Response(RESPONSE_TYPE.TABLE, data_frame=result_df)

        except ValueError:
            # Re-raise ValueError (e.g., invalid column name) as-is
            raise
        except Exception as e:
            logger.error(f"Failed to get column statistics: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        finally:
            if need_to_close:
                self.disconnect()

    def _extract_fields_from_mapping(self, properties: Dict, fields: Dict, prefix: str = "") -> None:
        """
        Recursively extracts field definitions from Elasticsearch mapping.

        This helper method flattens nested object and nested type fields into dot-notation paths.

        Args:
            properties (Dict): Field properties from mapping
            fields (Dict): Output dictionary to populate with field definitions
            prefix (str): Current field path prefix for nested fields
        """
        for field_name, field_def in properties.items():
            full_field_name = f"{prefix}.{field_name}" if prefix else field_name
            field_type = field_def.get("type")

            if field_type:
                # Regular field with a type
                fields[full_field_name] = field_def
            elif "properties" in field_def:
                # Nested object - recurse into it
                self._extract_fields_from_mapping(field_def["properties"], fields, full_field_name)
            else:
                # Field without type or properties (treat as object)
                fields[full_field_name] = {"type": "object"}

    def get_primary_keys(self, table_name: str) -> Response:
        """
        Retrieves the primary key for the specified Elasticsearch index.

        In Elasticsearch, the _id field serves as the implicit primary key for each document.
        This method always returns _id as the primary key.

        Args:
            table_name (str): The name of the index.

        Returns:
            Response: DataFrame with columns:
                - constraint_name: Name of the primary key constraint
                - column_name: The column name (_id)

        Example:
            >>> handler.get_primary_keys('products')
            # Returns: constraint_name='PRIMARY', column_name='_id'
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string")

        logger.debug(f"Getting primary keys for {table_name}")

        # Elasticsearch always uses _id as the document identifier (primary key)
        pk_data = [{"constraint_name": "PRIMARY", "column_name": "_id"}]

        return Response(RESPONSE_TYPE.TABLE, data_frame=DataFrame(pk_data))

    def get_foreign_keys(self, table_name: str) -> Response:
        """
        Retrieves foreign keys for the specified Elasticsearch index.

        Elasticsearch is a NoSQL document store and does not support foreign key constraints.
        This method always returns an empty DataFrame with the proper structure.

        Args:
            table_name (str): The name of the index.

        Returns:
            Response: Empty DataFrame with columns:
                - constraint_name: Foreign key constraint name
                - column_name: The column name
                - referenced_table: The referenced table name
                - referenced_column: The referenced column name

        Example:
            >>> handler.get_foreign_keys('products')
            # Returns: Empty DataFrame (NoSQL has no foreign keys)
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string")

        logger.debug(f"Getting foreign keys for {table_name} (NoSQL - will return empty)")

        # Elasticsearch is NoSQL and doesn't have foreign key constraints
        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=DataFrame(columns=["constraint_name", "column_name", "referenced_table", "referenced_column"]),
        )
