from typing import Text, Dict, Optional, List, Any
import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError,
    AuthenticationException,
    TransportError,
    RequestError,
)
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
            config["hosts"] = self.connection_data["hosts"].split(",")
        if "cloud_id" in self.connection_data:
            config["cloud_id"] = self.connection_data["cloud_id"]
        if "api_key" in self.connection_data:
            config["api_key"] = self.connection_data["api_key"]

        # Authentication
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

            # Handle pagination for large result sets
            while response.get("cursor"):
                response = connection.sql.query(body={"query": query, "cursor": response["cursor"]})
                if response["rows"]:
                    records.extend(response["rows"])
                else:
                    break

            column_names = [col["name"] for col in columns]
            if not records:
                records = [[None] * len(column_names)]

            return Response(RESPONSE_TYPE.TABLE, data_frame=DataFrame(records, columns=column_names))

        except (TransportError, RequestError) as e:
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

        # Execute search with pagination
        try:
            search_body = {
                "size": 1000,  # Reasonable batch size
                "query": {"match_all": {}},
            }

            response = self.connection.search(index=index_name, body=search_body, scroll="5m")

            records = []
            all_columns = set()
            scroll_id = response.get("_scroll_id")

            # Process results in batches
            while True:
                hits = response.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    doc = hit.get("_source", {})
                    if doc:
                        converted_doc = self._convert_arrays_to_strings(doc)
                        flattened_doc = self._flatten_document(converted_doc)
                        if flattened_doc:
                            records.append(flattened_doc)
                            all_columns.update(flattened_doc.keys())

                # Get next batch or break
                if not scroll_id:
                    break
                try:
                    response = self.connection.scroll(scroll_id=scroll_id, scroll="5m")
                    if not response.get("hits", {}).get("hits", []):
                        break
                except Exception:
                    break

            # Clean up scroll
            if scroll_id:
                try:
                    self.connection.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

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
