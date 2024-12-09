from mindsdb.integrations.handlers.instatus_handler.instatus_tables import StatusPages, Components
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
import requests
import pandas as pd
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

logger = log.getLogger(__name__)


class InstatusHandler(APIHandler):
    def __init__(self, name: str, **kwargs) -> None:
        """initializer method

        Args:
            name (str): handler name
        """
        super().__init__(name)
        self._base_url = "https://api.instatus.com"
        self._api_key = None

        args = kwargs.get('connection_data', {})
        if 'api_key' in args:
            self._api_key = args['api_key']

        self.connection = None
        self.is_connected = False

        _tables = [
            StatusPages,
            Components
        ]

        for Table in _tables:
            self._register_table(Table.name, Table(self))

    def check_connection(self) -> StatusResponse:
        """checking the connection

        Returns:
            StatusResponse: whether the connection is still up
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Instatus API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def connect(self) -> StatusResponse:
        # If already connected, return the existing connection
        if self.is_connected and self.connection:
            return self.connection

        if self._api_key:
            try:
                headers = {"Authorization": f"Bearer {self._api_key}"}
                response = requests.get(f"{self._base_url}/v2/pages", headers=headers)

                if response.status_code == 200:
                    self.connection = response
                    self.is_connected = True
                    return StatusResponse(True)
                else:
                    raise Exception(f"Error connecting to Instatus API: {response.status_code} - {response.text}")
            except requests.RequestException as e:
                raise Exception(f"Request to Instatus API failed: {str(e)}")

        raise Exception("API key is missing")

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

    def call_instatus_api(self, endpoint: str, method: str = 'GET', params: dict = None, json_data: dict = {}) -> pd.DataFrame:
        if not params:
            params = {}

        headers = {"Authorization": f"Bearer {self._api_key}"}
        url = f"{self._base_url}{endpoint}"

        if method.upper() in ('GET', 'POST', 'PUT', 'DELETE'):
            headers['Content-Type'] = 'application/json'

            response = requests.request(method, url, headers=headers, params=params, json=json_data)

            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            else:
                raise Exception(f"Error connecting to Instatus API: {response.status_code} - {response.text}")

        return pd.DataFrame()


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Instatus API key to use for authentication.",
        "required": True,
        "label": "Api key",
    },
)

connection_args_example = OrderedDict(
    api_key="d25509b171ad79395dc2c51b099ee6d0"
)
