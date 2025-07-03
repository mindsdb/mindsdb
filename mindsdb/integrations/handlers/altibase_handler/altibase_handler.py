from typing import Optional

import jaydebeapi as jdbcconnector
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode
import pandas as pd
import pyodbc
import numpy as np

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class AltibaseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Altibase statements.
    """

    name = 'altibase'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ constructor
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
        """
        super().__init__(name)

        self.parser = parse_sql

        self.connection_args = connection_data
        self.database = self.connection_args.get('database')
        self.host = self.connection_args.get('host')
        self.port = self.connection_args.get('port')
        self.user = self.connection_args.get('user')
        self.password = self.connection_args.get('password')
        self.dsn = self.connection_args.get('dsn')

        self.connection = None
        self.is_connected = False

    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting connection.
        Should switch self.is_connected.
        Returns:
            connection
        """
        if self.is_connected is True:
            return self.connection

        if self.dsn:
            return self.connect_with_odbc()
        else:
            return self.connect_with_jdbc()

    def connect_with_odbc(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting connection.
        Should switch self.is_connected.
        Returns:
            connection
        """
        conn_str = [f"DSN={self.dsn}"]

        if self.host:
            conn_str.append(f"Server={self.host}")
        if self.port:
            conn_str.append(f"Port={self.port}")
        if self.user:
            conn_str.append(f"User={self.user}")
        if self.password:
            conn_str.append(f"Password={self.password}")

        conn_str = ';'.join(conn_str)

        try:
            self.connection = pyodbc.connect(conn_str, timeout=10)
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to {self.database}, {e}")

        return self.connection

    def connect_with_jdbc(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting connection.
        Should switch self.is_connected.
        Returns:
            connection
        """
        jar_location = self.connection_args.get('jar_location')

        jdbc_class = self.connection_args.get('jdbc_class', 'Altibase.jdbc.driver.AltibaseDriver')
        jdbc_url = f"jdbc:Altibase://{self.host}:{self.port}/{self.database}"

        try:
            if self.user and self.password and jar_location:
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[self.user, self.password], jars=str(jar_location).split(","))
            elif self.user and self.password:
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, driver_args=[self.user, self.password])
            elif jar_location:
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url, jars=jar_location.split(","))
            else:
                connection = jdbcconnector.connect(jclassname=jdbc_class, url=jdbc_url)

            self.connection = connection
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error while connecting to {self.database}, {e}")
            raise e

        return self.connection

    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is True:
            try:
                self.connection.close()
                self.is_connected = False
            except Exception as e:
                logger.error(f"Error while disconnecting to {self.database}, {e}")
                return False
        return True

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        responseCode = StatusResponse(success=False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            logger.error(f'Error connecting to database {self.database}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success and need_to_close:
                self.disconnect()
            if not responseCode.success and self.is_connected:
                self.is_connected = False

        return responseCode

    def native_query(self, query: str) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False
        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()

                    if self.dsn:
                        if len(result) > 0:
                            result = np.array(result)

                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        if isinstance(query, ASTNode):
            query_str = query.to_string()
        else:
            query_str = str(query)

        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """ Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse
        """
        query = '''
            SELECT
                TABLE_NAME,
                TABLE_ID,
                TABLE_TYPE
            FROM
                system_.sys_tables_
            WHERE
                user_id = USER_ID();
            '''

        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
        """ Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """
        query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE
            FROM
                system_.sys_columns_ ct
            inner join
                system_.sys_tables_ tt
                on ct.table_id=tt.table_id
            where
                tt.table_name = '{table_name.capitalize()}';
            """

        return self.native_query(query)
