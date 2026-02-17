from mindsdb.integrations.handlers.intercom_handler.intercom_tables import Articles
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb_sql_parser import parse_sql
import requests
import pandas as pd
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import json
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class IntercomHandler(APIHandler):
    def __init__(self, name: str, **kwargs) -> None:
        """initializer method

        Args:
            name (str): handler name
        """
        super().__init__(name)

        self.connection = None
        self.is_connected = False
        self._baseUrl = 'https://api.intercom.io'
        args = kwargs.get('connection_data', {})
        if 'access_token' in args:
            access_token = args['access_token']
        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        self._register_table(Articles.name, Articles(self))

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
            logger.error(f'Error connecting to Intercom API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def connect(self) -> StatusResponse:
        """making the connectino object
        """
        if self.is_connected and self.connection:
            return self.connection

        if self._headers:
            try:
                response = requests.get(
                    url=self._baseUrl,
                    headers=self._headers
                )
                if response.status_code == 200:
                    self.connection = response
                    self.is_connected = True
                    return StatusResponse(True)
                else:
                    raise Exception(f"Error connecting to Intercom API: {response.status_code} - {response.text}")
            except requests.RequestException as e:
                raise Exception(f"Request to Intercom API failed: {str(e)}")

        raise Exception("Access token is missing")

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

    def call_intercom_api(self, endpoint: str, method: str = 'GET', params: dict = {}, data=None) -> pd.DataFrame:
        url = f"{self._baseUrl}{endpoint}"
        json_data = json.loads(data) if data else None

        response = requests.request(method.upper(), url, headers=self._headers, params=params, json=json_data)

        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame([data])
        else:
            raise requests.Response.raise_for_status(response)


connection_args = OrderedDict(
    access_token={
        "type": ARG_TYPE.PWD,
        "description": "Intercom access token to use for authentication.",
        "required": True,
        "label": "Access token",
    },
)

connection_args_example = OrderedDict(
    api_key="d25509b171ad79395dc2c51b099ee6d0"
)
