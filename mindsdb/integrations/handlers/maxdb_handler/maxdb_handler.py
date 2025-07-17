from typing import Optional

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE, HandlerResponse
)
import pandas as pd
import jaydebeapi as jd

logger = log.getLogger(__name__)


class MaxDBHandler(DatabaseHandler):
    """
       This handler handles connection and execution of the SAP MaxDB  statements.
       """

    name = 'maxdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.kwargs = kwargs
        self.parser = parse_sql
        self.connection_config = connection_data
        self.database = connection_data['database']
        self.host = connection_data['host']
        self.port = connection_data['port']
        self.user = connection_data['user']
        self.password = connection_data['password']
        self.jdbc_location = connection_data['jdbc_location']
        self.connection = None
        self.is_connected = False

    def __del__(self):
        """
        Destructor for the SAP MaxDB class.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Establishes a connection to the SAP MaxDB server.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return self.connection

        jdbc_url = f"jdbc:sapdb://{self.host}:{self.port}/{self.database}"
        jdbc_class = 'com.sap.dbtech.jdbc.DriverSapDB'

        self.connection = jd.connect(jdbc_class, jdbc_url, [self.user, self.password], self.jdbc_location)
        self.is_connected = True
        return self.connection

    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error while disconnecting to {self.database}, {e}")

        return

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to database {self.database}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> HandlerResponse:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        conn = self.connect()
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                self.connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()

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
        renderer = SqlalchemyRender("postgres")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Gets a list of table names in the database.

        Returns:
            list: A list of table names in the database.
        """

        query = f"SELECT TABLENAME FROM DOMAIN.TABLES WHERE TYPE = 'TABLE' AND SCHEMANAME = '{self.user}'"
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str) -> Response:
        """
        Gets a list of column names in the specified table.

        Args:
            table_name (str): The name of the table to get column names from.

        Returns:
            list: A list of column names in the specified table.
        """

        query = f"SELECT COLUMNNAME,DATATYPE FROM DOMAIN.COLUMNS WHERE TABLENAME ='{table_name}'"
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={'name': 'column_name', 'type': 'data_type'})
        return self.native_query(query)
