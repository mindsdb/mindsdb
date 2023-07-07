import os
import datetime as dt

import time
from collections import defaultdict
from typing import Any

import pandas as pd
import tweepy

from mindsdb.utilities import log
from mindsdb.utilities.config import Config


from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from tripadvisor_api import TripAdvisorAPI
from tripadvisor_table import SearchLocationTable


class TripAdvisorHandler(APIHandler):
    """A class for handling connections and interactions with the TripAdvisor Content API.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
        api (TripAdvisorAPI): The `TripAdvisorAPI` object for checking the connection to the Twitter API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self._tables = {}

        self.connection_args = {}
        handler_config = Config().get("tripadvisor_handler", {})
        for k in ["api_key"]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f"TRIPADVISOR_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"TRIPADVISOR_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        tripAdvisor = SearchLocationTable(self)
        self._register_table("searchLocationTable", tripAdvisor)

    def connect(self, api_version=2):
        """Check the connection with TripAdvisor API"""

        if self.is_connected is True:
            return self.api

        self.api = TripAdvisorAPI(api_key=self.connection_args["api_key"])

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        """This function evaluates if the connection is alive and healthy"""
        response = StatusResponse(False)

        try:
            api = self.connect()

            # make a random http call with searching a location.
            #   it raises an error in case if auth is not success and returns not-found otherwise
            api.connectTripAdvisor()
            response.success = True

        except Exception as e:
            response.error_message = f"Error connecting to TripAdvisor api: {e}"
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def _register_table(self, table_name: str, table_class: Any):
        """It registers the data resource in memory."""
        self._tables[table_name] = table_class
