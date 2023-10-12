import re
import os
import datetime as dt
import ast
import time
from collections import defaultdict
import pytz
import io
import requests

import pandas as pd
import shareplum

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
import json


class SharepointHandler(APIHandler):
    """
    The Stripe handler implementation.
    """

    name = 'sharepoint'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        lists_data = ListsTable(self)
        self._register_table("lists", listss_data)

        products_data = ProductsTable(self)
        self._register_table("products", products_data)

        payment_intents_data = PaymentIntentsTable(self)
        self._register_table("payment_intents", payment_intents_data)

    def connect(self):
        """
        Set up the context connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection
        client_creds = self.connection_data['client_id'], self.connection_data['client_secret'])
        ctx = ClientContext(self.connection_data['url']).with_credentials(client_creds)
        self.connection = ctx
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            ctx = self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Sharepoint!')
            response.error_message = str(e)

        self.is_connected = response.success

        return response

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
