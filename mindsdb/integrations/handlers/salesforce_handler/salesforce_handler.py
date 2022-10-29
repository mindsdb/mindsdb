from simple_salesforce import Salesforce

import requests
import pandas as pd
from io import StringIO

from typing import Optional
from collections import OrderedDict

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class SalesforceHandler(DatabaseHandler):
    """
    This handler handles connection and execution of Salesforce statements.
    """

    name = "salesforce"

    def __init__(self, name= str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'salesforce'
        self.renderer = SqlalchemyRender('salesforce')
        self.connection_data = connection_data
        self kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
                HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = Salesforce.client(
            'salesforce',
            sf_username=self.connection_data['user'],
            sf_password=self.connection_data['password'],
            sf_security_token=self.connection_data['security_token']
        )
        self.is_connected = True

        return self.connection

    def disconnect(self) -> StatusResponse:
         """ 
         Close any existing connections
        Should switch self.is_connected.
        """
        self.connection = None
        self.is_connected = False

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check if the connection is active.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = False

        try:
            connection = self.connection
            if connection is None:
                connection = self.connect()
                need_to_close = True

            response.success = True
        except Exception as e:
            response.error = str(e)
        finally:
            if need_to_close is True:
                self.disconnect()

        return response
    
    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and run it
        :param query: The SQL query to run in the database
        :return: returns the records from the dataset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                if ExecStatus(cursor.result.StatusResponse) == ExecStatus.COMMAND_OK:
                    response = Response(RESPONSE_TYPE.SUCCESS)
                    response.data = cursor.fetchall()
                else:
                    result = cursor.fetchall()
                    response = Response(RESPONSE_TYPE.TABLE)
                    response.data = pd.DataFrame(result)
            except Exception as e:
                response = Response(RESPONSE_TYPE.ERROR)
                response.error = str(e)
            finally:
                if need_to_close is True:
                    self.disconnect()

        return response

    def get_connection_params(self) -> OrderedDict:
        """
        Returns the connection parameters required by the handler.
        Returns:
            OrderedDict
        """
        return OrderedDict([
            ('user', ARG_TYPE.STRING),
            ('password', ARG_TYPE.STRING),
            ('security_token', ARG_TYPE.STRING)
        ])

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL query
        :param query: The SQL query to run in the database
        :return: returns the records from the dataset
        """
        query_str = self.renderer.render(query)
        return self.native_query(query_str)
        
    def get_tables(self) -> Response:
        """
        Retrieve the tables from the database
        :return: returns the tables from the database
        """
        query = 
        """
            SELECT
                table_schema,
                table_name
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('pg_catalog', 'information_schema')
                and table_type = ('BASE TABLE', 'VIEW')
        """

        return self.native_query(query)

    def get_columns(self, table: str) -> Response:
        """
        Retrieve the columns from the database
        :return: returns the columns from the database
        """
        query = f'SELECT * FROM {table}'
        return self.native_query(query)

    def get_sample(self, table: str, limit: int = 100) -> Response:
        """
        Retrieve the sample from the database
        :return: returns the sample from the database
        """
        query = f'SELECT * FROM {table} LIMIT {limit}'
        return self.native_query(query)

    connection_args = OrderedDict(
        protocol={
            'type': ARG_TYPE.STRING,
            'description': 'The protocol to use for the connection'
        },
        user= {
            'type': ARG_TYPE.STRING,
            'description': 'The username to use for the connection'
        },
        password={
            'type': ARG_TYPE.STRING,
            'description': 'The password to use for the connection'
        },
        security_token= {
            'type': ARG_TYPE.STRING,
            'description': 'The security token to use for the connection'
        }
        host= {
            'type': ARG_TYPE.STRING,
            'description': 'The host to use for the connection'
        },
        port= {
            'type': ARG_TYPE.STRING,
            'description': 'The port to use for the connection'
        }
    )