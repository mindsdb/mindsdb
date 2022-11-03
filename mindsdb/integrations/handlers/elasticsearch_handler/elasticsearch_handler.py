from typing import Optional
from collections import OrderedDict

import pandas as pd
from elasticsearch import Elasticsearch

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from es.elastic.sqlalchemy import ESDialect
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class ElasticsearchHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = 'elasticsearch'

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
        self.dialect = 'elasticsearch'

        if ('hosts' not in connection_data) and ('cloud_id' not in connection_data):
            raise Exception("Either the hosts or cloud_id parameter should be provided!")

        optional_parameters = ['hosts', 'cloud_id', 'username', 'password']
        for parameter in optional_parameters:
            if parameter not in connection_data:
                connection_data[parameter] = None

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

        self.connection = Elasticsearch(
            hosts=self.connection_data['hosts'].split(','),
            cloud_id=self.connection_data['cloud_id'],
            basic_auth=(self.connection_data['username'], self.connection_data['password'])
        )
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
            log.logger.error(f'Error connecting to Elasticsearch {self.connection_data["hosts"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            response = connection.sql.query(body={'query': query})
            records = response['rows']
            columns = response['columns']

            new_records = True
            while new_records:
                try:
                    if response['cursor']:
                        response = connection.sql.query(body={'query': query, 'cursor': response['cursor']})

                        new_records = response['rows']
                        records = records + new_records
                except Exception as e:
                    new_records = False

            if records:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        records,
                        columns=[column['name'] for column in columns]
                    )
                )
        except Exception as e:
            log.logger.error(f'Error running query: {query} on {self.connection_data["hosts"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        renderer = SqlalchemyRender(ESDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of indexes that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = """
            SHOW TABLES
        """
        result = self.native_query(query)
        df = result.data_frame
        df = df.drop(['type', 'type'], axis=1)
        result.data_frame = df.rename(columns={'name': 'table_name'})

        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of indexes returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"""
            DESCRIBE {table_name}
        """
        result = self.native_query(query)
        df = result.data_frame
        df = df.drop('mapping', axis=1)
        result.data_frame = df.rename(columns={'column': 'column_name', 'type': 'data_type'})

        return result


connection_args = OrderedDict(
    hosts={
        'type': ARG_TYPE.STR,
        'description': 'The host name(s) or IP address(es) of the Elasticsearch server(s). If multiple host name(s) or '
                       'IP address(es) exist, they should be separated by commas. This parameter is '
                       'optional, but it should be provided if cloud_id is not.'
    },
    cloud_id={
        'type': ARG_TYPE.STR,
        'description': 'The unique ID to your hosted Elasticsearch cluster on Elasticsearch Service. This parameter is '
                       'optional, but it should be provided if hosts is not.'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Elasticsearch server. This parameter is optional.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Elasticsearch server. This parameter is '
                       'optional.'
    }
)

connection_args_example = OrderedDict(
    hosts='localhost:9200',
)