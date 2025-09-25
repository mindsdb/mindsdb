from typing import List
from mindsdb.integrations.handlers.strapi_handler.strapi_tables import StrapiTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse, RESPONSE_TYPE, HandlerStatusResponse as StatusResponse
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities import log
import requests
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict
import pandas as pd

logger = log.getLogger(__name__)


class StrapiHandler(APIHandler):
    # Class-level caches
    _connection_cache = {}
    _table_schemas = {}  # Cache table schemas to avoid repeated API calls

    def __init__(self, name: str, **kwargs) -> None:
        """initializer method

        Args:
            name (str): handler name
        """
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        # Handle both complete URLs and host+port combinations
        if "url" in args and args.get("url"):
            # Complete URL provided (e.g., https://my-strapi.herokuapp.com)
            self._base_url = args.get("url").rstrip("/")
        elif "host" in args and args.get("host"):
            # Traditional host + port setup
            host = args.get("host", "")
            port = args.get("port", "")

            # Determine protocol
            protocol = "https" if args.get("ssl", False) else "http"

            if port:
                self._base_url = f"{protocol}://{host}:{port}"
            else:
                self._base_url = f"{protocol}://{host}"
        else:
            self._base_url = None
        self._api_token = args.get("api_token")
        self._plural_api_ids = args.get("plural_api_ids", [])

        self._connection_key = f"{self._base_url}_{self._api_token}"

        # Use cached connection status
        self.is_connected = self._connection_cache.get(self._connection_key, False)

        # Register tables but defer schema fetching
        for pluralApiId in self._plural_api_ids:
            table_instance = StrapiTable(handler=self, name=pluralApiId, defer_schema_fetch=True)
            self._register_table(table_name=pluralApiId, table_class=table_instance)

    def check_connection(self) -> StatusResponse:
        """checking the connection

        Returns:
            StatusResponse: whether the connection is still up
        """
        if self._connection_cache.get(self._connection_key, False):
            self.is_connected = True
            return StatusResponse(True)
        return self.connect()

    def connect(self) -> StatusResponse:
        """making the connectino object
        """
        if self._connection_cache.get(self._connection_key, False):
            self.is_connected = True
            return StatusResponse(True)

        try:
            headers = {"Authorization": f"Bearer {self._api_token}"}
            response = requests.get(f"{self._base_url}", headers=headers)
            if response.status_code == 200:
                self.is_connected = True
                self._connection_cache[self._connection_key] = True
                return StatusResponse(True)
            else:
                raise Exception(f"Error connecting to Strapi API: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f'Error connecting to Strapi API: {e}!')
            self._connection_cache[self._connection_key] = False
            return StatusResponse(False, error_message=e)

    def get_tables(self) -> HandlerResponse:
        """
        Return list of available Strapi collections
        Returns:
            RESPONSE_TYPE.TABLE
        """
        result = self._plural_api_ids

        df = pd.DataFrame(result, columns=["table_name"])
        df["table_type"] = "BASE TABLE"

        return HandlerResponse(RESPONSE_TYPE.TABLE, df)

    def get_table(self, table_name: str):
        """Create table instance on demand"""
        if table_name in self._plural_api_ids:
            return StrapiTable(handler=self, name=table_name)
        raise ValueError(f"Table {table_name} not found in your Strapi collections.")

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.

        Parameters
        ----------
        query : str
            query in a native format

        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query)
        return self.query(ast)

    def call_strapi_api(self, method: str, endpoint: str, params: dict = {}, json_data: dict = {}) -> pd.DataFrame:
        headers = {"Content-Type": "application/json"}
        # Add Authorization header only if API token is provided
        if self._api_token:
            headers["Authorization"] = f"Bearer {self._api_token}"

        url = f"{self._base_url}{endpoint}"

        if method.upper() in ("GET", "POST", "PUT", "DELETE"):
            if method.upper() in ("POST", "PUT", "DELETE"):
                response = requests.request(method, url, headers=headers, params=params, data=json_data)
            else:
                response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200 or response.status_code == 201:
                response_data = response.json()
                # Create an empty DataFrame
                df = pd.DataFrame()
                if isinstance(response_data.get("data", None), list):
                    df = pd.DataFrame(response_data["data"])
                elif isinstance(response_data.get("data", None), dict):
                    df = pd.DataFrame([response_data["data"]])
                return df
            else:
                raise Exception(f"Error connecting to Strapi API: {response.status_code} - {response.text}")

        return pd.DataFrame()


connection_args = OrderedDict(
    api_token={
        "type": ARG_TYPE.PWD,
        "description": "Strapi API key to use for authentication.",
        "required": True,
        "label": "Api token",
    },
    host={
        "type": ARG_TYPE.URL,
        "description": "Strapi API host to connect to.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Strapi API port to connect to.",
        "required": True,
        "label": "Port",
    },
    plural_api_ids={
        "type": list,
        "description": "Plural API id to use for querying.",
        "required": True,
        "label": "Plural API id",
    },
)

connection_args_example = OrderedDict(
    host="localhost",
    port=1337,
    api_token="c56c000d867e95848c",
    plural_api_ids=["posts", "portfolios"],
)
