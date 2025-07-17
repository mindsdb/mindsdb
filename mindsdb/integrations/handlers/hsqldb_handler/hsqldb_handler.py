import pandas as pd
import pyodbc

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

logger = log.getLogger(__name__)


class HSQLDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the HyperSQL statements.
    """

    name = 'hsqldb'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'hsqldb'
        self.connection_args = kwargs.get('connection_data')
        self.server_name = self.connection_args.get('server_name', 'localhost')
        self.port = self.connection_args.get('port')
        self.database_name = self.connection_args.get('database_name')
        self.username = self.connection_args.get('username')
        self.password = self.connection_args.get('password')
        self.conn_str = f"DRIVER={{PostgreSQL Unicode}};SERVER={self.server_name};PORT={self.port};DATABASE={self.database_name};UID={self.username};PWD={self.password};Trusted_Connection=True"
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

        self.connection = pyodbc.connect(self.conn_str, timeout=10)
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
            logger.error(f'Error connecting to SQLite, {e}!')
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
                        data_frame=pd.DataFrame.from_records(
                            result,
                            columns=[x[0] for x in cursor.description]
                        )
                    )

                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query}!')
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

        renderer = SqlalchemyRender('postgres')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type='BASE TABLE'")
        results = cursor.fetchall()
        df = pd.DataFrame([x[2] for x in results], columns=['table_name'])  # Workaround since cursor.tables() wont work with postgres driver
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        connection = self.connect()
        cursor = connection.cursor()
        query = f'SELECT * FROM information_schema.columns WHERE table_name ={table_name}'  # Workaround since cursor.columns() wont work with postgres driver
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(
            [(x[3], x[7]) for x in results],
            columns=['column_name', 'data_type']
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response
