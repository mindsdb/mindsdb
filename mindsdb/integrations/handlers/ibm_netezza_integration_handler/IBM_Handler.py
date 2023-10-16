from typing import Optional
from collections import OrderedDict
import pandas as pd
import pyodbc
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

class IBMNetezzaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of IBM Netezza statements.
    """

    name = 'ibm_netezza'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of a particular handler instance.
            connection_data (dict): parameters for connecting to the Netezza database.
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'netezza'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return StatusResponse(True)

        connection_string = (
            f"Driver={{{self.connection_data['driver']}}};"
            f"Server={self.connection_data['db_host']};"
            f"Port={self.connection_data['db_port']};"
            f"Database={self.connection_data['db_name']};"
            f"UID={self.connection_data['db_user']};"
            f"PWD={self.connection_data['db_password']}"
        )

        try:
            self.connection = pyodbc.connect(connection_string)
            self.is_connected = True
            return StatusResponse(True)
        except pyodbc.Error as e:
            log.logger.error(f'Error connecting to IBM Netezza: {str(e)}')
            return StatusResponse(False, error_message=str(e))

    def disconnect(self):
        """
        Close any existing connections.
        """
        if self.is_connected:
            self.connection.close()
            self.is_connected = False
        return StatusResponse(True)

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to IBM Netezza: {str(e)}')
            response.error_message = str(e)
        finally:
            if response.success and need_to_close:
                self.disconnect()
            if not response.success and self.is_connected:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format.
        Returns:
            HandlerResponse
        """
        need_to_close = not self.is_connected

        connection = self.connect()
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame.from_records(
                            result,
                            columns=[x[0] for x in cursor.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on IBM Netezza!')
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST.
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender(AccessDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Return a list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """
        connection = self.connect()
        with connection.cursor() as cursor:
            df = pd.DataFrame([table.table_name for table in cursor.tables(tableType='Table')], columns=['table_name'])

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of the tables returned by self.get_tables().
        Returns:
            HandlerResponse
        """
        connection = self.connect()
        with connection.cursor() as cursor:
            df = pd.DataFrame(
                [(column.column_name, column.type_name) for column in cursor.columns(table=table_name)],
                columns=['column_name', 'data_type']
            )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response


connection_args = OrderedDict(
    db_host={
        'type': ARG_TYPE.STR,
        'description': 'The hostname or IP address of the Netezza server.'
    },
    db_port={
        'type': ARG_TYPE.INT,
        'description': 'The port number for the Netezza database.'
    },
    db_name={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Netezza database.'
    },
    db_user={
        'type': ARG_TYPE.STR,
        'description': 'The username for connecting to Netezza.'
    },
    db_password={
        'type': ARG_TYPE.STR,
        'description': 'The password for the Netezza user.'
    },
    driver={
        'type': ARG_TYPE.STR,
        'description': 'The ODBC driver name for Netezza (e.g., NetezzaSQL).'
    }
)

connection_args_example = OrderedDict(
    db_host='your_netezza_server',
    db_port=5480,
    db_name='your_database_name',
    db_user='your_username',
    db_password='your_password',
    driver='NetezzaSQL'
)
