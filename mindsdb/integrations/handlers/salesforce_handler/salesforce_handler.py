# Set up Python package and API access
from simple_salesforce import Salesforce
import requests
import json
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

sf = Salesforce(username='username', password='password',
                security_token='security_token')


class SalesforceHandler(DatabaseHandler):
    """
     This handler handles connection and execution of Salesforce statements.
    """

    name = "salesforce"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler
        Args:
            name (str): name of particular handler instance
                        connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'salesforce'
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
            return StatusResponse(True)

        self.connection = sf.client(
            'salesforce',
            sf_username=self.connection_data['user'],
            sf_password=self.connection_data['password'],
            sf_security_token=self.connection_data['security_token']
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
        self.connection = None
        self.is_connected = False

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the Salesforce connection
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False
        if need_to_close is True:
            self.connect()
        try:
            self.connection.query("SELECT Id FROM Account LIMIT 1")
            response = StatusResponse(True)
        except Exception as e:
            response = StatusResponse(False, error=str(e))
        if need_to_close is True:
            self.disconnect()
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SOQL query from Salesforce and return the result as a pandas dataframe
        Args: 
            query (str): SOQL query
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        connection = self.connection

        if need_to_close is True:
            self.connect()
            connection = self.connection

        try:
            result = connection.select_object_content(
                sf_instance=self.connection_data['sf_instance'],
                reportId=self.connection_data['reportId'],
                export='?isdtp=p1&export=1&enc=UTF-8&xf=csv',
                sfUrl=sf_instance + reportId + export,
                response=requests.get(sfUrl, headers=headers)
                download=response.content.decode('utf-8'),
                df=pd.read_csv(StringIO(download))
            )
            response = Response(RESPONSE_TYPE.SUCCESS, result)
        except Exception as e:
            response = Response(RESPONSE_TYPE.ERROR, str(e))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive a query from MindsDB and return the result as a pandas dataframe
        Args:
            query (ASTNode): MindsDB query
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender(Salesforce)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list of tables in the database
        """
        query = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
        """
        return self.native_query(query)

    def get_columns(self, table_name):
        query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        return self.native_query(query)

    connection_args = OrderedDict(
        protocol={
            'type': ARG_TYPE.STRING,
            'description': 'The protocol to use for the connection'
        },
        user={
            'type': ARG_TYPE.STRING,
            'description': 'The username to use for the connection'
        },
        password={
            'type': ARG_TYPE.STRING,
            'description': 'The password to use for the connection'
        },
        security_token={
            'type': ARG_TYPE.STRING,
            'description': 'The security token to use for the connection'
        }
        host={
            'type': ARG_TYPE.STRING,
            'description': 'The host to use for the connection'
        },
        port={
            'type': ARG_TYPE.STRING,
            'description': 'The port to use for the connection'
        }
    )
