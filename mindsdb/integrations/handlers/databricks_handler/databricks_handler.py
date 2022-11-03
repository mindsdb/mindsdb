from typing import Optional
from collections import OrderedDict

import pandas as pd
from databricks import sql

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from sqlalchemy_databricks import DatabricksDialect
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class DatabricksHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Databricks statements.
    """

    name = 'databricks'

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
        self.dialect = 'databricks'

        optional_parameters = ['session_configuration', 'http_headers', 'catalog', 'schema']
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

        self.connection = sql.connect(
            server_hostname=self.connection_data['server_hostname'],
            http_path=self.connection_data['http_path'],
            access_token=self.connection_data['access_token'],
            session_configuration=self.connection_data['session_configuration'],
            http_headers=self.connection_data['http_headers'],
            catalog=self.connection_data['catalog'],
            schema=self.connection_data['schema']
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
            log.logger.error(f'Error connecting to Databricks {self.connection_data["schema"]}, {e}!')
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
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cursor.description]
                        )
                    )

                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.connection_data["schema"]}!')
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
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender(DatabricksDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = """
            SHOW TABLES;
        """
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={'tableName': 'table_name'})
        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"DESCRIBE {table_name};"
        result = self.native_query(query)
        df = result.data_frame

        drop_row = df[df['col_name'] == ''].index.tolist()[0]
        df = df.iloc[:drop_row + 1]

        result.data_frame = df.rename(columns={'col_name': 'column_name'})
        return result


connection_args = OrderedDict(
    server_hostname={
        'type': ARG_TYPE.STR,
        'description': 'The server hostname for the cluster or SQL warehouse.'
    },
    http_path={
        'type': ARG_TYPE.STR,
        'description': 'The HTTP path of the cluster or SQL warehouse.'
    },
    access_token={
        'type': ARG_TYPE.STR,
        'description': 'A Databricks personal access token for the workspace for the cluster or SQL warehouse.'
    },
    session_configuration={
        'type': ARG_TYPE.STR,
        'description': 'A dictionary of Spark session configuration parameters. This parameter is optional.'
    },
    http_headers={
        'type': ARG_TYPE.STR,
        'description': 'Additional (key, value) pairs to set in HTTP headers on every RPC request the client makes.'
                       ' This parameter is optional.'
    },
    catalog={
        'type': ARG_TYPE.STR,
        'description': 'Catalog to use for the connection. This parameter is optional.'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'Schema (database) to use for the connection. This parameter is optional.'
    }
)

connection_args_example = OrderedDict(
    server_hostname='adb-1234567890123456.7.azuredatabricks.net',
    http_path='sql/protocolv1/o/1234567890123456/1234-567890-test123',
    access_token='dapi1234567890ab1cde2f3ab456c7d89efa',
    schema='sales'
)