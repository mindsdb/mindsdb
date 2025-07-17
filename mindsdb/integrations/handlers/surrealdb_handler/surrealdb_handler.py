from typing import Optional
import pysurrealdb as surreal
import pandas as pd

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from .utils.surreal_get_info import table_names, column_info


logger = log.getLogger(__name__)


class SurrealDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SurrealDB statements.
    """
    name = 'surrealdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.database = connection_data['database']
        self.parser = parse_sql
        self.dialect = "surrealdb"
        self.kwargs = kwargs
        self.namespace = connection_data['namespace']
        self.user = connection_data['user']
        self.password = connection_data['password']
        self.host = connection_data['host']
        self.port = connection_data['port']

        self.connection = None
        self.is_connected = False

    def connect(self):
        """
        Establishes a connection to the MindsDB database.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection
        try:
            self.connection = surreal.connect(
                database=self.database,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                namespace=self.namespace,
            )
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to SurrealDB, {e}")

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False
        try:
            self.connect()
            response_code.success = True
        except Exception as e:
            logger.error(f'Error connecting to SurrealDB, {e}!')
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False

        return response_code

    def disconnect(self):
        """
        Close the existing connection to the SurrealDB database
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error while disconnecting to SurrealDB, {e}")

        return

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in SurrealQL to execute
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.execute(query)
            result = cur.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description],
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f'Error running query: {query} on SurrealDB!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        cur.close()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. It may be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        query_string = query.to_string()

        # ensure the correct query is passed
        last_word = query_string.split()[-1]
        query_string = query_string.replace(last_word + '.', "")
        return self.native_query(query_string)

    def get_tables(self) -> Response:
        """
        Get list of tables from the database that will be accessible.
        Returns:
            HandlerResponse
        """
        conn = self.connect()
        # get table names
        tables = table_names(conn)

        # construct pandas dataframe
        df = pd.DataFrame(tables, columns=['table_name'])

        response = Response(
            RESPONSE_TYPE.TABLE, df
        )
        return response

    def get_columns(self, table: str) -> Response:
        """ Return list of columns in table
        Args:
            table (str): name of the table to get column names and types from.
        Returns:
            HandlerResponse
        """
        conn = self.connect()
        # get name and type of each column in the table
        columns, types = column_info(conn, table)

        # construct pandas dataframe
        df = pd.DataFrame(columns, columns=['table_name'])
        df['data_type'] = types

        response = Response(
            RESPONSE_TYPE.TABLE, df
        )
        return response
