import os
import time
from typing import Any, Dict

import pandas as pd
import webzio
from dotty_dict import dotty
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.webz_handler.webz_tables import (
    WebzPostsTable,
    WebzReviewsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)


class WebzHandler(APIHandler):
    """A class for handling connections and interactions with the Webz API."""

    API_CALL_EXEC_LIMIT_SECONDS = 60
    AVAILABLE_CONNECTION_ARGUMENTS = ["token"]

    def __init__(self, name: str = None, **kwargs):
        """Registers all tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self.connection_args = self._read_connection_args(name, **args)

        self.client = None
        self.is_connected = False
        self.max_page_size = 100

        self._register_table(WebzPostsTable.TABLE_NAME, WebzPostsTable(self))
        self._register_table(WebzReviewsTable.TABLE_NAME, WebzReviewsTable(self))

    def _read_connection_args(self, name: str = None, **kwargs) -> Dict[str, Any]:
        """Read the connection arguments by following the order of precedence below:

        1. PARAMETERS object
        2. Environment Variables
        3. MindsDB Config File

        """
        filtered_args = {}
        handler_config = Config().get(f"{name.lower()}_handler", {})
        for k in type(self).AVAILABLE_CONNECTION_ARGUMENTS:
            if k in kwargs:
                filtered_args[k] = kwargs[k]
            elif f"{name.upper()}_{k.upper()}" in os.environ:
                filtered_args[k] = os.environ[f"{name.upper()}_{k.upper()}"]
            elif k in handler_config:
                filtered_args[k] = handler_config[k]
        return filtered_args

    def connect(self) -> object:
        """Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected and self.client is not None:
            return self.client

        webzio.config(token=self.connection_args["token"])
        self.client = webzio
        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        try:
            webzio_client = self.connect()
            webzio_client.query("filterWebContent", {"q": "AI", "size": 1})
            response.success = True
        except Exception as e:
            response.error_message = f"Error connecting to Webz api: {e}."

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str = None) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """
        ast = parse_sql(query)
        return self.query(ast)

    def _parse_item(self, item, output_colums):
        dotted_item = dotty(item)
        return {field.replace(".", "__"): dotted_item[field] for field in output_colums}

    def call_webz_api(
        self, method_name: str = None, params: Dict = None
    ) -> pd.DataFrame:
        """Calls the API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call
            params (Dict): Params to pass to the API call
        """
        table_name = method_name
        table = self._tables[table_name]

        client = self.connect()

        left = None
        count_results = None

        data = []
        limit_exec_time = time.time() + type(self).API_CALL_EXEC_LIMIT_SECONDS

        if "size" in params:
            count_results = params["size"]

        #  GET param q is mandatory, so in order to collect all data,
        # it's needed to use as a query an asterisk (*)
        if "q" not in params:
            params["q"] = "*"

        while True:
            if time.time() > limit_exec_time:
                raise RuntimeError("Handler request timeout error")

            if count_results is not None:
                left = count_results - len(data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    data = data[:left]
                    break

                if left > self.max_page_size:
                    params["size"] = self.max_page_size
                else:
                    params["size"] = left

            logger.debug(
                f"Calling Webz API: {table.ENDPOINT} with params ({params})"
            )

            output = (
                client.query(table.ENDPOINT, params)
                if len(data) == 0
                else client.get_next()
            )
            for item in output.get(table_name, []):
                data.append(self._parse_item(item, table.OUTPUT_COLUMNS))

        df = pd.DataFrame(data)
        return df
