from typing import Text, Dict, Optional, List, Any
import json
import re

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError,
    AuthenticationException,
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
    This handler handles the connection and execution of SQL statements on Elasticsearch.
    """

    name = "elasticsearch"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Elasticsearch cluster.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self._index_mappings_cache = {}
        self._array_fields_cache = {}

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> Elasticsearch:
        """
        Establishes a connection to the Elasticsearch host.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            elasticsearch.Elasticsearch: A connection object to the Elasticsearch host.
        """
        if self.is_connected is True:
            return self.connection

        config = {}

        # Mandatory connection parameters.
        if ("hosts" not in self.connection_data) and ("cloud_id" not in self.connection_data):
            raise ValueError("Either the hosts or cloud_id parameter should be provided!")

        # Optional/Additional connection parameters.
        optional_parameters = ["hosts", "cloud_id", "api_key"]
        for parameter in optional_parameters:
            if parameter in self.connection_data:
                if parameter == "hosts":
                    config["hosts"] = [host.strip() for host in self.connection_data[parameter].split(",")]
                else:
                    config[parameter] = self.connection_data[parameter]

        # Ensure that if either user or password is provided, both are provided.
        if ("user" in self.connection_data) != ("password" in self.connection_data):
            raise ValueError("Both user and password should be provided if one of them is provided!")

        if "user" in self.connection_data:
            config["http_auth"] = (
                self.connection_data["user"],
                self.connection_data["password"],
            )

        try:
            self.connection = Elasticsearch(
                **config,
            )
            self.is_connected = True
            return self.connection
        except ConnectionError as conn_error:
            logger.error(f"Connection error when connecting to Elasticsearch: {conn_error}")
            raise
        except AuthenticationException as auth_error:
            logger.error(f"Authentication error when connecting to Elasticsearch: {auth_error}")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknown error when connecting to Elasticsearch: {unknown_error}")
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the Elasticsearch host if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def _detect_array_fields(self, index_name: str) -> List[str]:
        """
        Detect fields that contain arrays by sampling documents from the index.

        Args:
            index_name (str): The name of the index to analyze

        Returns:
            List[str]: List of field paths that contain arrays
        """
        if index_name in self._array_fields_cache:
            return self._array_fields_cache[index_name]

        array_fields = []
        try:
            connection = self.connect()

            # Get a few sample documents to detect arrays
            response = connection.search(index=index_name, body={"size": 5, "query": {"match_all": {}}}, _source=True)

            for hit in response.get("hits", {}).get("hits", []):
                sample_doc = hit.get("_source", {})
                array_fields.extend(self._find_arrays_in_document(sample_doc))

            # Remove duplicates and cache result
            array_fields = list(set(array_fields))
            self._array_fields_cache[index_name] = array_fields

            if array_fields:
                logger.info(f"Detected array fields in {index_name}: {array_fields}")

        except Exception as e:
            logger.error(f"Error detecting array fields in {index_name}: {e}")

        return array_fields

    def _find_arrays_in_document(self, doc: Dict, prefix: str = "") -> List[str]:
        """
        Recursively find array fields in a document.

        Args:
            doc (Dict): The document to analyze
            prefix (str): Current field path prefix

        Returns:
            List[str]: List of field paths containing arrays
        """
        array_fields = []

        if not isinstance(doc, dict):
            return array_fields

        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key

            if isinstance(value, list):
                array_fields.append(field_path)
            elif isinstance(value, dict):
                array_fields.extend(self._find_arrays_in_document(value, field_path))

        return array_fields

    def _use_search_api_fallback(
        self,
        index_name: str,
        limit: int = 50,
        where_clause: Optional[str] = None,
        selected_columns: Optional[List[str]] = None,
    ) -> Response:
        """
        Fallback to Elasticsearch Search API when SQL API fails due to arrays.

        Args:
            index_name (str): The index to query
            limit (int): Maximum number of documents to return
            where_clause (str): Optional WHERE clause conditions

        Returns:
            Response: Query response with converted array data
        """
        try:
            connection = self.connect()

            # Build query body with optional filtering
            query_body = {"size": limit, "query": {"match_all": {}}}

            # Simple WHERE clause support for basic conditions
            if where_clause and "=" in where_clause and "!=" not in where_clause:
                parts = where_clause.split("=", 1)
                if len(parts) == 2:
                    field_name = parts[0].strip()
                    field_value = parts[1].strip().strip("'").strip('"')

                    # Convert value type
                    try:
                        field_value = int(field_value)
                    except ValueError:
                        try:
                            field_value = float(field_value)
                        except ValueError:
                            if field_value.lower() in ("true", "false"):
                                field_value = field_value.lower() == "true"

                    query_body["query"] = {"term": {field_name: field_value}}

            # Use scroll API for large result sets
            if limit > 1000:
                response = connection.search(
                    index=index_name, body=query_body, scroll="2m", size=min(1000, limit), _source=True
                )
                scroll_id = response.get("_scroll_id")
            else:
                response = connection.search(index=index_name, body=query_body, _source=True)
                scroll_id = None

            # Process documents and convert arrays to JSON strings
            records = []
            all_columns = set()
            processed_count = 0

            while True:
                hits = response.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    if processed_count >= limit:
                        break

                    try:
                        doc = hit.get("_source", {})
                        if not doc:  # Skip empty documents
                            continue

                        converted_doc = self._convert_arrays_to_strings(doc)
                        flattened_doc = self._flatten_document(converted_doc)

                        if flattened_doc:  # Only process non-empty flattened docs
                            records.append(flattened_doc)
                            all_columns.update(flattened_doc.keys())
                            processed_count += 1
                    except Exception as doc_error:
                        logger.warning(f"Error processing document {processed_count}: {doc_error}")
                        continue

                if processed_count >= limit or not scroll_id:
                    break

                # Get next batch
                response = connection.scroll(scroll_id=scroll_id, scroll="2m")

            # Clean up scroll
            if scroll_id:
                try:
                    connection.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass  # Ignore cleanup errors

            # Handle column selection with robust error handling
            try:
                if selected_columns:
                    # Filter columns to only requested ones
                    available_columns = [col for col in selected_columns if col in all_columns]
                    if not available_columns:
                        # If none of the requested columns exist, return available columns
                        columns = sorted(list(all_columns))[:5] if all_columns else ["no_data"]
                        logger.warning(
                            f"None of requested columns {selected_columns} found, returning available columns"
                        )
                    else:
                        columns = available_columns
                else:
                    # Return all columns if no specific selection
                    columns = sorted(list(all_columns)) if all_columns else ["no_data"]

                normalized_records = []

                for i, record in enumerate(records):
                    try:
                        normalized_record = [record.get(col) for col in columns]
                        normalized_records.append(normalized_record)
                    except Exception as record_error:
                        logger.warning(f"Error processing record {i}: {record_error}")
                        # Create a safe record with None values
                        safe_record = [None] * len(columns)
                        normalized_records.append(safe_record)

            except Exception as column_error:
                logger.error(f"Error in column processing: {column_error}")
                # Fallback to basic structure
                columns = ["error_data"]
                normalized_records = [["Error processing columns"]]

            if not normalized_records or not columns:
                # Return empty result with consistent structure
                if not columns:
                    columns = ["no_data"]
                normalized_records = [[None] * len(columns)]

            df = DataFrame(normalized_records, columns=columns)

            logger.info(f"Successfully retrieved {len(records)} documents from {index_name} using Search API fallback")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Search API fallback failed for {index_name}: {e}")
            logger.error(f"Exception details: {type(e).__name__}: {str(e)}")

            # Return empty result instead of error for better UX
            empty_df = DataFrame([[None]], columns=["no_data"])
            logger.info(f"Returning empty result for {index_name} due to error")
            return Response(RESPONSE_TYPE.TABLE, data_frame=empty_df)

    def _convert_arrays_to_strings(self, obj: Any) -> Any:
        """
        Convert array fields to JSON strings for SQL compatibility.
        Enhanced with null handling and type preservation.

        Args:
            obj: The object to process

        Returns:
            The processed object with arrays converted to strings
        """
        if obj is None:
            return None
        elif isinstance(obj, list):
            # Handle empty arrays and nested structures
            if len(obj) == 0:
                return "[]"
            try:
                return json.dumps(obj, ensure_ascii=False, default=str)
            except (TypeError, ValueError) as e:
                logger.warning(f"Could not serialize array to JSON: {e}")
                return str(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_arrays_to_strings(v) for k, v in obj.items()}
        elif isinstance(obj, (int, float, bool)):
            return obj
        elif isinstance(obj, str):
            return obj
        else:
            # Handle other types (dates, etc.)
            return str(obj)

    def _flatten_document(self, doc: Dict, prefix: str = "") -> Dict:
        """
        Flatten nested document structure for tabular representation.

        Args:
            doc (Dict): The document to flatten
            prefix (str): Current field path prefix

        Returns:
            Dict: Flattened document
        """
        flattened = {}

        if not isinstance(doc, dict):
            return {prefix: doc} if prefix else {"value": doc}

        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                flattened.update(self._flatten_document(value, field_path))
            else:
                flattened[field_path] = value

        return flattened

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Elasticsearch host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()

            # Execute a simple query to test the connection.
            connection.sql.query(body={"query": "SELECT 1"})
            response.success = True
        # All exceptions are caught here to ensure that the connection is closed if an error occurs.
        except Exception as error:
            logger.error(f"Error connecting to Elasticsearch, {error}!")
            response.error_message = str(error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Elasticsearch host and returns the result.
        Enhanced with array field detection and automatic fallback to Search API.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        # Extract table name and check for array fields if this is a SELECT query
        index_name = self._extract_index_name_from_query(query)

        # Proactive array detection for SELECT queries
        if index_name and "SELECT" in query.upper():
            array_fields = self._detect_array_fields(index_name)
            if array_fields:
                logger.info(f"Proactively detected array fields {array_fields} in {index_name}, using Search API")
                limit = self._extract_limit_from_query(query)
                columns = self._extract_columns_from_query(query)
                return self._use_search_api_fallback(index_name, limit, selected_columns=columns)

        try:
            response = connection.sql.query(body={"query": query})
            records = response.get("rows", [])
            columns = response.get("columns", [])

            # Handle pagination
            new_records = True
            while new_records:
                try:
                    if response.get("cursor"):
                        response = connection.sql.query(body={"cursor": response["cursor"]})
                        new_records = response.get("rows", [])
                        if new_records:
                            records = records + new_records
                        else:
                            new_records = False
                    else:
                        new_records = False
                except KeyError:
                    new_records = False

            column_names = (
                [column.get("name", f"col_{i}") for i, column in enumerate(columns)] if columns else ["no_data"]
            )
            if not records:
                null_record = [None] * len(column_names)
                records = [null_record]

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=DataFrame(records, columns=column_names),
            )

        except Exception as transport_or_request_error:
            error_message = str(transport_or_request_error)

            # Check if this is an array-related error that needs fallback
            array_error_keywords = ["Arrays", "array", "IllegalArgumentException"]
            fallback_needed = any(keyword in error_message for keyword in array_error_keywords)

            if fallback_needed:
                logger.info(f"SQL query failed due to array fields, attempting Search API fallback for query: {query}")

                if index_name:
                    # Extract LIMIT and columns if present
                    limit = self._extract_limit_from_query(query)
                    columns = self._extract_columns_from_query(query)
                    return self._use_search_api_fallback(index_name, limit, selected_columns=columns)
                else:
                    logger.warning("Could not extract index name for Search API fallback")

            logger.error(f"Error running query: {query} on Elasticsearch, {transport_or_request_error}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=error_message)

        except Exception as unknown_error:
            logger.error(f"Unknown error running query: {unknown_error}")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(unknown_error))

        if need_to_close is True:
            self.disconnect()

        return response

    def _extract_index_name_from_query(self, query: str) -> Optional[str]:
        """
        Extract the index name from a SQL query.

        Args:
            query (str): The SQL query

        Returns:
            Optional[str]: The index name if found
        """
        try:
            # Simple regex to extract table name from FROM clause
            match = re.search(r"FROM\s+([^\s;,]+)", query, re.IGNORECASE)
            if match:
                return match.group(1).strip("`\"'")
        except Exception as e:
            logger.debug(f"Could not extract index name from query: {e}")

        return None

    def _extract_columns_from_query(self, query: str) -> Optional[List[str]]:
        """Extract column names from SELECT statement."""
        try:
            # Handle SELECT * case
            if "SELECT *" in query.upper():
                return None

            # Find SELECT and FROM positions in a case-insensitive way
            query_upper = query.upper()
            select_pos = query_upper.find("SELECT")
            from_pos = query_upper.find("FROM")

            if select_pos == -1 or from_pos == -1:
                return None

            # Extract column list from original query (preserving case)
            select_part = query[select_pos + 6 : from_pos].strip()  # 6 = len("SELECT")

            # Remove backticks and split by comma
            columns = []
            for col in select_part.split(","):
                col = col.strip().strip("`").strip("'").strip('"')
                if col:
                    columns.append(col)

            return columns if columns else None
        except Exception as e:
            logger.debug(f"Failed to extract columns from query: {e}")
            return None

    def _extract_limit_from_query(self, query: str) -> int:
        """
        Extract LIMIT value from a SQL query.

        Args:
            query (str): The SQL query

        Returns:
            int: The limit value, defaults to 50
        """
        try:
            match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except Exception as e:
            logger.debug(f"Could not extract limit from query: {e}")

        return 50  # Default limit

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Elasticsearch host and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(ESDialect)
        query_str = renderer.get_string(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables (indexes) in the Elasticsearch host.

        Returns:
            Response: A response object containing a list of tables (indexes) in the Elasticsearch host.
        """
        try:
            result = self._get_tables_fallback()

            if result.type == RESPONSE_TYPE.TABLE and result.data_frame is not None and len(result.data_frame) > 0:
                return result
            else:
                # Direct Elasticsearch connection as fallback
                connection = self.connect()
                indices = connection.cat.indices(format="json")

                table_data = []
                for index in indices:
                    index_name = index.get("index", "")
                    if not index_name.startswith("."):  # Filter system indices
                        table_data.append([index_name, "TABLE"])

                if table_data:
                    df = DataFrame(table_data, columns=["table_name", "table_type"])
                    return Response(RESPONSE_TYPE.TABLE, data_frame=df)
                else:
                    # Return empty DataFrame with proper structure
                    df = DataFrame([{"table_name": None, "table_type": None}])
                    return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Error in get_tables: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve tables: {str(e)}")

    def _get_tables_fallback(self) -> Response:
        """
        Fallback method to get tables using Elasticsearch's native API.

        Returns:
            Response: Response containing table information
        """
        try:
            connection = self.connect()

            # Get all indices using the cat API
            indices = connection.cat.indices(format="json")

            # Filter out system indices and create DataFrame
            table_data = []
            for index in indices:
                index_name = index.get("index", "")
                if not index_name.startswith("."):
                    table_data.append({"table_name": index_name, "table_type": "TABLE"})

            if not table_data:
                # Return empty DataFrame with proper structure
                table_data = [{"table_name": None, "table_type": None}]

            df = DataFrame(table_data)

            logger.info(f"Successfully retrieved {len(table_data)} tables using fallback method")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Fallback get_tables failed: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve tables: {str(e)}")

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column (field) details for a specified table (index) in the Elasticsearch host.
        Enhanced with proper error handling and array field information.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"DESCRIBE {table_name}"
        result = self.native_query(query)

        # Handle error cases
        if result.type == RESPONSE_TYPE.ERROR:
            logger.error(f"DESCRIBE query failed for {table_name}: {result.error_message}")
            return self._get_columns_fallback(table_name)

        if result.data_frame is None or result.data_frame.empty:
            logger.warning(f"DESCRIBE returned empty result for {table_name}, trying fallback")
            return self._get_columns_fallback(table_name)

        try:
            df = result.data_frame.copy()

            # Safely drop columns that might not exist
            if "mapping" in df.columns:
                df = df.drop("mapping", axis=1)

            # Ensure proper column renaming
            column_mapping = {}
            if "column" in df.columns:
                column_mapping["column"] = "column_name"
            if "type" in df.columns:
                column_mapping["type"] = "data_type"

            if column_mapping:
                df = df.rename(columns=column_mapping)

            # Add array field detection
            array_fields = self._detect_array_fields(table_name)
            if array_fields and "column_name" in df.columns:
                df["is_array"] = df["column_name"].isin(array_fields)
            else:
                df["is_array"] = False

            result.data_frame = df
            return result

        except Exception as e:
            logger.error(f"Error processing DESCRIBE result for {table_name}: {e}")
            return self._get_columns_fallback(table_name)

    def _get_columns_fallback(self, table_name: str) -> Response:
        """
        Fallback method to get columns using Elasticsearch's mapping API.

        Args:
            table_name (str): The name of the table/index

        Returns:
            Response: Response containing column information
        """
        try:
            connection = self.connect()

            # Get mapping for the index
            mapping = connection.indices.get_mapping(index=table_name)

            if table_name not in mapping:
                return Response(RESPONSE_TYPE.ERROR, error_message=f"Index '{table_name}' not found")

            properties = mapping[table_name]["mappings"].get("properties", {})

            # Convert mapping to column information
            column_data = []
            array_fields = self._detect_array_fields(table_name)

            def extract_fields(props, prefix=""):
                for field_name, field_info in props.items():
                    full_field_name = f"{prefix}.{field_name}" if prefix else field_name

                    field_type = field_info.get("type", "object")
                    is_array = full_field_name in array_fields

                    column_data.append({"column_name": full_field_name, "data_type": field_type, "is_array": is_array})

                    # Handle nested objects
                    if "properties" in field_info:
                        extract_fields(field_info["properties"], full_field_name)

            extract_fields(properties)

            if not column_data:
                column_data = [{"column_name": None, "data_type": None, "is_array": False}]

            df = DataFrame(column_data)

            logger.info(f"Successfully retrieved {len(column_data)} columns for {table_name} using fallback method")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Fallback get_columns failed for {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve columns for {table_name}: {str(e)}")
