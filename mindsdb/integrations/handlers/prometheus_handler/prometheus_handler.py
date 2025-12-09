from typing import Optional, List, Dict, Any
import pandas as pd
import requests
from datetime import datetime, timedelta

from mindsdb.integrations.handlers.prometheus_handler.prometheus_tables import (
    MetricsTable,
    LabelsTable,
    ScrapeTargetsTable,
    MetricDataTable,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


def _map_type(data_type: str) -> MYSQL_DATA_TYPE:
    """Map Prometheus data types to MySQL types.

    Args:
        data_type (str): The data type name

    Returns:
        MYSQL_DATA_TYPE: The corresponding MySQL data type
    """
    if data_type is None:
        return MYSQL_DATA_TYPE.VARCHAR

    data_type_upper = data_type.upper()

    type_map = {
        "VARCHAR": MYSQL_DATA_TYPE.VARCHAR,
        "TEXT": MYSQL_DATA_TYPE.TEXT,
        "INTEGER": MYSQL_DATA_TYPE.INT,
        "INT": MYSQL_DATA_TYPE.INT,
        "BIGINT": MYSQL_DATA_TYPE.BIGINT,
        "DECIMAL": MYSQL_DATA_TYPE.DECIMAL,
        "FLOAT": MYSQL_DATA_TYPE.FLOAT,
        "DOUBLE": MYSQL_DATA_TYPE.DOUBLE,
        "BOOLEAN": MYSQL_DATA_TYPE.BOOL,
        "BOOL": MYSQL_DATA_TYPE.BOOL,
        "TIMESTAMP": MYSQL_DATA_TYPE.DATETIME,
        "DATETIME": MYSQL_DATA_TYPE.DATETIME,
        "ARRAY": MYSQL_DATA_TYPE.TEXT,
    }

    return type_map.get(data_type_upper, MYSQL_DATA_TYPE.VARCHAR)


class PrometheusHandler(MetaAPIHandler):
    """Prometheus API handler implementation"""

    name = "prometheus"

    def __init__(self, name: str, **kwargs: Any) -> None:
        """
        Initialize the handler.

        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments including connection_data
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.base_url = connection_data.get("prometheus_url", "http://localhost:9090")
        # Remove trailing slash if present
        self.base_url = self.base_url.rstrip("/")
        self.timeout = connection_data.get("timeout", 10)
        
        # Authentication
        self.username = connection_data.get("username")
        self.password = connection_data.get("password")
        self.bearer_token = connection_data.get("bearer_token")
        
        # Validate authentication parameters
        if self.username and not self.password:
            raise ValueError("Password is required when username is provided")
        if self.bearer_token and (self.username or self.password):
            logger.warning("Both bearer_token and username/password provided. Bearer token will be used.")

        self.is_connected: bool = False

        # Register tables for data catalog
        metrics_table = MetricsTable(self)
        self._register_table("metrics", metrics_table)

        labels_table = LabelsTable(self)
        self._register_table("labels", labels_table)

        scrape_targets_table = ScrapeTargetsTable(self)
        self._register_table("scrape_targets", scrape_targets_table)

        metric_data_table = MetricDataTable(self)
        self._register_table("metric_data", metric_data_table)

    def connect(self) -> bool:
        """Creates a connection to Prometheus API.

        Returns:
            bool: True if connection successful

        Raises:
            Exception: If connection to Prometheus API fails.
        """
        if self.is_connected:
            return True

        try:
            # Prepare authentication
            auth = None
            headers = {}
            
            if self.bearer_token:
                headers["Authorization"] = f"Bearer {self.bearer_token}"
            elif self.username and self.password:
                from requests.auth import HTTPBasicAuth
                auth = HTTPBasicAuth(self.username, self.password)
            
            # Test connection by querying the /api/v1/status/config endpoint
            response = requests.get(
                f"{self.base_url}/api/v1/status/config",
                auth=auth,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            self.is_connected = True
            logger.info("Successfully connected to Prometheus API")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to Prometheus API: {str(e)}")
            raise Exception(f"Connection to Prometheus failed: {str(e)}")

    def disconnect(self) -> None:
        """Close connection and cleanup resources."""
        self.is_connected = False
        logger.info("Disconnected from Prometheus API")

    def check_connection(self) -> StatusResponse:
        """Checks whether the API client is connected to Prometheus.

        Returns:
            StatusResponse: A status response indicating whether the API client is connected.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
            logger.info("Prometheus connection check successful")
        except Exception as e:
            logger.error(f"Prometheus connection check failed: {str(e)}")
            response.error_message = str(e)
            response.success = False

        self.is_connected = response.success
        return response

    def native_query(self, query: Optional[str] = None) -> Response:
        """Receive and process a raw query.

        Args:
            query (str): query in a native format (SQL)

        Returns:
            Response: Response containing query results or error information

        Raises:
            ValueError: If query is None or empty
            Exception: If query parsing or execution fails
        """
        if not query:
            return Response(RESPONSE_TYPE.ERROR, error_message="Query cannot be None or empty")

        try:
            ast = parse_sql(query)
            return self.query(ast)
        except Exception as e:
            logger.error(f"Failed to execute native query: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Query execution failed: {str(e)}")

    def get_tables(self) -> Response:
        """Return list of tables available in the Prometheus integration.

        Returns:
            Response: A response containing table metadata including table names, types,
            estimated row counts, and descriptions.
        """
        try:
            self.connect()

            tables_data = [
                {
                    "TABLE_SCHEMA": "prometheus",
                    "TABLE_NAME": "metrics",
                    "TABLE_TYPE": "BASE TABLE",
                },
                {
                    "TABLE_SCHEMA": "prometheus",
                    "TABLE_NAME": "labels",
                    "TABLE_TYPE": "BASE TABLE",
                },
                {
                    "TABLE_SCHEMA": "prometheus",
                    "TABLE_NAME": "scrape_targets",
                    "TABLE_TYPE": "BASE TABLE",
                },
                {
                    "TABLE_SCHEMA": "prometheus",
                    "TABLE_NAME": "metric_data",
                    "TABLE_TYPE": "BASE TABLE",
                },
            ]

            df = pd.DataFrame(tables_data)
            logger.info(f"Retrieved metadata for {len(tables_data)} table(s)")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Failed to get tables: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve table list: {str(e)}")

    def get_columns(self, table_name: str) -> Response:
        """Return column information for a specific table in standard information_schema.columns format.

        Args:
            table_name (str): Name of the table to get column information for

        Returns:
            Response: A response containing column metadata in standard information_schema.columns format.
        """
        if table_name not in ["metrics", "labels", "scrape_targets", "metric_data"]:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Table '{table_name}' not found. Available tables: metrics, labels, scrape_targets, metric_data",
            )

        try:
            self.connect()

            # Get columns from the table implementation
            table = self._tables.get(table_name)
            if table:
                columns = table.get_columns()
            else:
                # Fallback column definitions
                columns = self._get_default_columns(table_name)

            # Transform to information_schema.columns format
            columns_data = []
            for idx, col in enumerate(columns, start=1):
                col_name = col if isinstance(col, str) else col.get("COLUMN_NAME", "")
                data_type = "VARCHAR" if isinstance(col, str) else col.get("DATA_TYPE", "VARCHAR")

                columns_data.append(
                    {
                        "COLUMN_NAME": col_name,
                        "DATA_TYPE": data_type,
                        "ORDINAL_POSITION": idx,
                        "COLUMN_DEFAULT": None,
                        "IS_NULLABLE": "YES",
                        "CHARACTER_MAXIMUM_LENGTH": None,
                        "CHARACTER_OCTET_LENGTH": None,
                        "NUMERIC_PRECISION": None,
                        "NUMERIC_SCALE": None,
                        "DATETIME_PRECISION": None,
                        "CHARACTER_SET_NAME": None,
                        "COLLATION_NAME": None,
                    }
                )

            df = pd.DataFrame(columns_data)
            logger.info(f"Retrieved {len(columns_data)} columns for table {table_name}")

            result = Response(RESPONSE_TYPE.TABLE, data_frame=df)
            result.to_columns_table_response(map_type_fn=_map_type)
            return result

        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {str(e)}")
            return Response(
                RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve columns for table '{table_name}': {str(e)}"
            )

    def _get_default_columns(self, table_name: str) -> List[str]:
        """Get default column names for a table."""
        defaults = {
            "metrics": ["metric_name", "type", "help", "unit"],
            "labels": ["metric_name", "label", "job", "instance", "method", "status", "custom_labels"],
            "scrape_targets": ["target_labels", "health", "scrape_url"],
            "metric_data": ["pql_query", "start_ts", "end_ts", "step", "timeout"],
        }
        return defaults.get(table_name, [])

    def call_prometheus_api(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a request to the Prometheus API.

        Args:
            endpoint (str): API endpoint (e.g., '/api/v1/label/__name__/values')
            params (dict): Query parameters

        Returns:
            dict: API response JSON
        """
        if not self.is_connected:
            self.connect()

        url = f"{self.base_url}{endpoint}"
        
        # Prepare authentication
        auth = None
        headers = {}
        
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        elif self.username and self.password:
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(self.username, self.password)
        
        response = requests.get(url, params=params, auth=auth, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

