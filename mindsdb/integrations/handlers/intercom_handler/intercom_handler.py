from mindsdb.integrations.handlers.intercom_handler.intercom_tables import Articles, Admins
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb_sql import parse_sql
import requests
import pandas as pd
from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import json
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class IntercomHandler(APIHandler):
    def __init__(self, name: str, **kwargs) -> None:
        """Initializer method for the IntercomHandler class.

        Args:
            name (str): The name of the handler.
            connection_data (dict): A dictionary containing the connection data.
                It should have the following key-value pair:
                - 'access_token' (str): The access token for the Intercom API.
        """
        super().__init__(name)

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
        self._register_table(Admins.name, Admins(self))

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
        """making the connection object

        Connects to the Intercom API using the provided access token.
        If the connection is already established, it returns the existing connection object.
        If the connection is not established, it makes a request to the Intercom API to retrieve user information.

        Returns:
            StatusResponse: An object indicating the status of the connection.

        Raises:
            Exception: If the request to the Intercom API fails or the access token is missing.
        """
        if self.is_connected:
            return StatusResponse(True)

        if self._headers:
            try:
                self.call_intercom_api(endpoint='/me')
                self.is_connected = True
                return StatusResponse(True)
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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

    def call_intercom_api(self, endpoint: str, method: str = 'GET', params: dict = {}, data=None) -> pd.DataFrame:
        """
        Calls the Intercom API with the specified endpoint, method, params, and data.

        Args:
            endpoint (str): The API endpoint to call.
            method (str, optional): The HTTP method to use. Defaults to 'GET'.
            params (dict, optional): The query parameters to include in the request. Defaults to {}.
            data (Any, optional): The data to include in the request body. Defaults to None.

        Returns:
            pd.DataFrame: The response data as a pandas DataFrame.

        Raises:
            Exception: If the API call returns a non-200 status code, an exception is raised with the error message.
        """
        url = f"{self._baseUrl}{endpoint}"
        json_data = json.loads(data) if data else None

        response = requests.request(method.upper(), url, headers=self._headers, params=params, json=json_data)

        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame([data])
        else:
            raise Exception(response.json()['errors'][0]['message'])


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
