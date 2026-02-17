import requests
from typing import Any, Dict, List, Optional

from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.gong_handler.gong_tables import (
    GongCallsTable,
    GongUsersTable,
    GongAnalyticsTable,
    GongTranscriptsTable,
)
from mindsdb.integrations.handlers.gong_handler.constants import (
    get_gong_api_info,
    GONG_TABLES_METADATA,
    GONG_PRIMARY_KEYS,
    GONG_FOREIGN_KEYS,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class GongHandler(MetaAPIHandler):
    """
    This handler handles the connection and execution of SQL statements on Gong.
    """

    name = "gong"

    def __init__(self, name: str, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Gong API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.base_url = connection_data.get("base_url", "https://api.gong.io")
        self.timeout = connection_data.get("timeout", 30)  # Default 30 second timeout

        # Support both bearer token and access key + secret key
        self.bearer_token = connection_data.get("api_key")
        self.access_key = connection_data.get("access_key")
        self.secret_key = connection_data.get("secret_key")

        # Register core tables
        self._register_table("calls", GongCallsTable(self))
        self._register_table("users", GongUsersTable(self))
        self._register_table("analytics", GongAnalyticsTable(self))
        self._register_table("transcripts", GongTranscriptsTable(self))

    def connect(self) -> requests.Session:
        """
        Establishes a connection to the Gong API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            Exception: If a connection error occurs.

        Returns:
            requests.Session: A session object for making API requests.
        """
        if self.is_connected is True:
            return self.connection

        if self.access_key and self.secret_key:
            auth_method = "basic"
        elif self.bearer_token:
            auth_method = "bearer"
        else:
            raise ValueError("Either bearer_token or (access_key + secret_key) is required to connect to Gong API.")

        try:
            self.connection = requests.Session()

            if auth_method == "basic":
                # Basic authentication with access key + secret key
                self.connection.auth = (self.access_key, self.secret_key)
                self.connection.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
            else:
                # Bearer token authentication
                self.connection.headers.update(
                    {
                        "Authorization": f"Bearer {self.bearer_token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    }
                )

            test_response = self.connection.get(f"{self.base_url}/v2/users", timeout=self.timeout)
            test_response.raise_for_status()

            self.is_connected = True
            return self.connection

        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting to Gong API: {e}")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Gong API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            # Test the connection by making a simple API call
            test_response = self.connection.get(f"{self.base_url}/v2/users")
            test_response.raise_for_status()
            response.success = True
        except Exception as e:
            logger.error(f"Connection check to Gong failed: {e}")
            response.error_message = str(e)

        self.is_connected = response.success
        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a native query on Gong and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        try:
            ast = parse_sql(query)
            return self.query(ast)
        except Exception as e:
            logger.error(f"Error running query: {query} on Gong: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_code=0, error_message=str(e))

    def call_gong_api(self, endpoint: str, method: str = "GET", params: Dict = None, json: Dict = None) -> Dict:
        """
        Makes a call to the Gong API.

        Args:
            endpoint (str): The API endpoint to call.
            method (str): HTTP method (GET or POST).
            params (Dict): Query parameters for the API call (for GET requests).
            json (Dict): JSON payload for POST requests.

        Returns:
            Dict: The API response.
        """
        if not self.is_connected:
            self.connect()

        url = f"{self.base_url}{endpoint}"

        if method.upper() == "POST":
            response = self.connection.post(url, json=json, timeout=self.timeout)
        else:
            response = self.connection.get(url, params=params, timeout=self.timeout)

        response.raise_for_status()
        return response.json()

    def meta_get_handler_info(self, **kwargs) -> str:
        """
        Retrieves information about the Gong API handler design and implementation.

        Returns:
            str: A string containing information about the handler's design and implementation.
        """
        return get_gong_api_info(self.name)

    def meta_get_tables(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves metadata for the specified tables (or all tables if no list is provided).

        Note: Gong API doesn't provide a metadata/schema discovery endpoint, so we use
        the handler's registered tables combined with static metadata from constants.

        Args:
            table_names (List): A list of table names for which to retrieve metadata.

        Returns:
            Response: A response object containing the table metadata.
        """
        import pandas as pd

        metadata_list = []

        # Get metadata for requested tables (or all if none specified)
        # Use registered tables to ensure we only return tables that are actually available
        for table_name in self._tables.keys():
            if (table_names is None or table_name in table_names) and table_name in GONG_TABLES_METADATA:
                metadata = GONG_TABLES_METADATA[table_name]
                metadata_list.append(
                    {
                        "table_name": metadata["name"],
                        "table_type": metadata["type"],
                        "description": metadata["description"],
                        "api_endpoint": metadata["api_endpoint"],
                        "supports_pagination": metadata["supports_pagination"],
                        "notes": metadata.get("notes", ""),
                    }
                )

        df = pd.DataFrame(metadata_list)
        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_columns(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Note: Column schemas are derived from the table's get_columns() method when available,
        falling back to static metadata from constants.

        Args:
            table_names (List): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        import pandas as pd

        column_metadata_list = []

        # Get column metadata for requested tables (or all if none specified)
        for table_name in self._tables.keys():
            if (table_names is None or table_name in table_names) and table_name in GONG_TABLES_METADATA:
                metadata = GONG_TABLES_METADATA[table_name]

                # Try to get live columns from the table class
                table_instance = self._tables[table_name]
                if hasattr(table_instance, "get_columns"):
                    live_columns = table_instance.get_columns()

                    # Match live columns with metadata
                    for column_name in live_columns:
                        # Find column metadata
                        column_meta = next(
                            (col for col in metadata["columns"] if col["name"] == column_name),
                            {"type": "str", "description": f"Column {column_name}"},
                        )

                        column_metadata_list.append(
                            {
                                "table_name": table_name,
                                "column_name": column_name,
                                "data_type": column_meta.get("type", "str"),
                                "description": column_meta.get("description", ""),
                                "is_filterable": column_name in metadata.get("filterable_columns", []),
                            }
                        )
                else:
                    # Fallback to static metadata
                    for column in metadata["columns"]:
                        column_metadata_list.append(
                            {
                                "table_name": table_name,
                                "column_name": column["name"],
                                "data_type": column["type"],
                                "description": column["description"],
                                "is_filterable": column["name"] in metadata.get("filterable_columns", []),
                            }
                        )

        df = pd.DataFrame(column_metadata_list)
        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves primary key metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve primary key metadata.

        Returns:
            Response: A response object containing the primary key metadata.
        """
        import pandas as pd

        pk_list = []

        # Get primary key metadata for requested tables (or all if none specified)
        for table_name in self._tables.keys():
            if (table_names is None or table_name in table_names) and table_name in GONG_PRIMARY_KEYS:
                pk_info = GONG_PRIMARY_KEYS[table_name]
                pk_list.append(
                    {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": pk_info["column_name"],
                        "CONSTRAINT_NAME": pk_info["constraint_name"],
                    }
                )

        df = pd.DataFrame(pk_list)
        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves foreign key metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve foreign key metadata.

        Returns:
            Response: A response object containing the foreign key metadata.
        """
        import pandas as pd

        fk_list = []

        # Get foreign key metadata for requested tables (or all if none specified)
        for table_name in self._tables.keys():
            if (table_names is None or table_name in table_names) and table_name in GONG_FOREIGN_KEYS:
                for fk_info in GONG_FOREIGN_KEYS[table_name]:
                    fk_list.append(
                        {
                            "TABLE_NAME": table_name,
                            "COLUMN_NAME": fk_info["column_name"],
                            "FOREIGN_TABLE_NAME": fk_info["foreign_table_name"],
                            "FOREIGN_COLUMN_NAME": fk_info["foreign_column_name"],
                            "CONSTRAINT_NAME": fk_info["constraint_name"],
                        }
                    )

        df = pd.DataFrame(fk_list)
        return Response(RESPONSE_TYPE.TABLE, df)
