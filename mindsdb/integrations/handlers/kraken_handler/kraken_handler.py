import pandas as pd
import requests
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.handlers.kraken_handler.kraken_tables import KrakenTradesTable
from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse


class KrakenHandler(APIHandler):
    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)
        self.api_key = None
        self.api_secret = None

        args = kwargs.get("connection_data", {})
        if "api_key" in args:
            self.api_key = args["api_key"]
        if "api_secret" in args:
            self.api_secret = args["api_secret"]
        self.client = None
        self.is_connected = False

        trade_history = KrakenTradesTable(self)
        self._register_table("kraken_trade_history", trade_history)

    def connect(self):
        """Creates a new Kraken API client if needed and sets it as the client to use for requests.
        Returns newly created Kraken API client, or current client if already set.
        """
        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Checks connection to Kraken API by sending a ping request.
        Returns StatusResponse indicating whether or not the handler is connected.
        """
        response = StatusResponse(True)
        self.is_connected = response.success
        return response

    def get_trade_history(self, queries: dict) -> pd.DataFrame:
        api_url = "https://api.kraken.com"
        uri_path = "/0/private/TradesHistory"
        headers = {}
        headers["API-Key"] = self.api_key
        headers["API-Sign"] = self.api_secret
        res = requests.post((api_url + uri_path), headers=headers, data=queries)
        result = res["result"]
        # error = res["error"]
        trades = result["trades"]
        trades_list = []
        for trade_id, details in trades:
            temp = details
            temp["trade_id"] = trade_id
            trades_list.append(temp)
        df = pd.DataFrame(trades_list)
        return df

    def call_kraken_api(self, method_name, params):
        if method_name == "get_trade_history":
            return self.get_trade_history(params)
        raise NotImplementedError(
            "Method name {} not supported by Kraken API Handler".format(method_name)
        )


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "API Key For Connecting to Kraken API.",
        "required": True,
        "label": "API Key",
    },
    api_secret={
        "type": ARG_TYPE.PWD,
        "description": "API Secret For Connecting to Kraken API.",
        "required": True,
        "label": "API Secret",
    },
)

connection_args_example = OrderedDict(api_key="public_key", api_secret="secret_key")
