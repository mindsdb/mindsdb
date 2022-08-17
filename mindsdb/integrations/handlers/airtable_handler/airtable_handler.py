from typing import Optional
from collections import OrderedDict

import pandas as pd
import requests
import duckdb

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base_handler import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class AirtableHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Firebird statements.
    """

    name = 'airtable'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'airtable'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        url = f"https://api.airtable.com/v0/{self.connection_data['base_id']}/{self.connection_data['table_name']}"
        headers = {"Authorization": "Bearer " + self.connection_data['api_key']}

        response = requests.get(url, headers=headers)
        response = response.json()
        records = response['records']

        while new_records:
            try:
                if response['offset']:
                    params = {"offset": response['offset']}
                    response = requests.get(url, params=params, headers=headers)
                    response = response.json()

                    new_records = response['records']
                    records = records + new_records
            except Exception as e:
                new_records = False

        rows = [record['fields'] for record in records]
        globals()[self.connection_data['table_name']] = pd.DataFrame(rows)

        self.connection = duckdb.connect()

        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to Airtable base {self.connection_data["base_id"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response