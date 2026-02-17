import pyodbc

import pandas as pd
from mindsdb_sql_parser import parse_sql
from ingres_sa_dialect.base import IngresDialect
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


logger = log.getLogger(__name__)


class IngresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Ingres statements.
    """

    name = 'ingres'

    def __init__(self, name: str, **kwargs):
        """
        Initializes a new instance of the Ingres handler.

        Args:
            name (str): The name of the database.
            **kwargs: parameters for connecting to the database
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'ingres'
        self.connection_args = kwargs.get('connection_data')
        self.database = self.connection_args.get('database')
        self.server = self.connection_args.get('server')
        self.user = self.connection_args.get('user')
        self.password = self.connection_args.get('password')
        self.servertype = self.connection_args.get('servertype', 'ingres')
        self.connection = None
        self.is_connected = False

    def __del__(self):
        """
        Destructor for the Ingres class.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to the Ingres server.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return self.connection

        conn_str = f"Driver={{Ingres}};Server={self.server};Database={self.database};UID={self.user};" \
                   f"PWD={self.password};ServerType={self.servertype}"

        self.connection = pyodbc.connect(conn_str)
        self.is_connected = True
        return self.connection

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
            logger.error(f'Error connecting to Ingres, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def disconnect(self):
        """
        Closes the connection to the Ingres server.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): SQL query to execute.
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
                        data_frame=pd.DataFrame.from_records(
                            result,
                            columns=[x[0] for x in cursor.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.connection_args["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        renderer = SqlalchemyRender(IngresDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Gets a list of table names in the database.

        Returns:
            list: A list of table names in the database.
        """
        connection = self.connect()
        cursor = connection.cursor()
        # Execute query to get all table names
        cursor.execute(
            "SELECT table_name FROM iitables WHERE table_type = 'T'")

        table_names = [x[0] for x in cursor.fetchall()]

        # Create dataframe with table names
        df = pd.DataFrame(table_names, columns=['table_name', 'data_type'])

        # Create response object
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Gets a list of column names in the specified table.

        Args:
            table_name (str): The name of the table to get column names from.

        Returns:
            list: A list of column names in the specified table.
        """
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT column_name FROM iicolumns WHERE table_name = '{}'".format(table_name))
        results = cursor.fetchall()

        # construct a pandas dataframe from the query results
        df = pd.DataFrame(
            results,
            columns=['column_name', 'data_type']
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response
