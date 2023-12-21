from mindsdb.integrations.handlers.strapi_handler.strapi_tables import StrapiTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb_sql import parse_sql
import requests
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict
import pandas as pd

logger = log.getLogger(__name__)


class StrapiHandler(APIHandler):
    def __init__(self, name: str, **kwargs) -> None:
        """initializer method

        Args:
            name (str): handler name
        """
        super().__init__(name)

        self.is_connected = False
        args = kwargs.get('connection_data', {})
        if 'host' in args and 'port' in args:
            self._base_url = f"http://{args['host']}:{args['port']}/api"
        if 'api_token' in args:
            self._api_token = args['api_token']
        if 'plural_api_ids' in args:
            self._plural_api_ids = args['plural_api_ids']
        # Registers tables for each collections in strapi
        for pluralApiId in self._plural_api_ids:
            self._register_table(table_name=pluralApiId, table_class=StrapiTable(handler=self, name=pluralApiId))

    def check_connection(self) -> StatusResponse:
        """checking the connection

        Returns:
            StatusResponse: whether the connection is still up
        """
        return self.connect()

    def connect(self) -> StatusResponse:
        """making the connectino object
        """
        if self.is_connected:
            return StatusResponse(True)

        try:
            self.call_strapi_api(method='GET', endpoint='/users')
            self.is_connected = True
        except Exception as e:
            self.is_connected = False
            return StatusResponse(False, error_message=e)

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

    def call_strapi_api(self, method: str, endpoint: str, params: dict = {}, json_data: dict = {}) -> pd.DataFrame:
        headers = {"Authorization": f"Bearer {self._api_token}"}
        url = f"{self._base_url}{endpoint}"

        if method.upper() in ('GET', 'POST', 'PUT', 'DELETE'):
            headers['Content-Type'] = 'application/json'
            response = requests.request(method, url, headers=headers, params=params, data=json_data)

            if response.status_code == 200:
                data = response.json()
                # Create an empty DataFrame
                df = pd.DataFrame()
                if isinstance(data.get('data', None), list):
                    for item in data['data']:
                        # Add 'id' and 'attributes' to the DataFrame
                        row_data = {'id': item['id'], **item['attributes']}
                        df = df._append(row_data, ignore_index=True)
                elif isinstance(data.get('data', None), dict):
                    # Add 'id' and 'attributes' to the DataFrame
                    row_data = {'id': data['data']['id'], **data['data']['attributes']}
                    df = df._append(row_data, ignore_index=True)
            else:
                raise Exception(response.json()['error']['message'])
        return df


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
