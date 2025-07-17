from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class ClickHouseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the ClickHouse statements.
    """

    name = 'clickhouse'

    def __init__(self, name, connection_data, **kwargs):
        super().__init__(name)
        self.dialect = 'clickhouse'
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(ClickHouseDialect)
        self.is_connected = False
        self.protocol = connection_data.get('protocol', 'native')

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to a ClickHouse server using SQLAlchemy.

        Raises:
            SQLAlchemyError: If an error occurs while connecting to the database.

        Returns:
            Connection: A SQLAlchemy Connection object to the ClickHouse database.
        """
        if self.is_connected:
            return self.connection

        protocol = "clickhouse+native" if self.protocol == 'native' else "clickhouse+http"
        host = quote(self.connection_data['host'])
        port = self.connection_data['port']
        user = quote(self.connection_data['user'])
        password = quote(self.connection_data['password'])
        database = quote(self.connection_data['database'])
        url = f'{protocol}://{user}:{password}@{host}:{port}/{database}'
        # This is not redundunt. Check https://clickhouse-sqlalchemy.readthedocs.io/en/latest/connection.html#http
        if self.protocol == 'https':
            url = url + "?protocol=https"
        try:
            engine = create_engine(url)
            connection = engine.raw_connection()
            self.is_connected = True
            self.connection = connection
        except SQLAlchemyError as e:
            logger.error(f'Error connecting to ClickHouse {self.connection_data["database"]}, {e}!')
            self.is_connected = False
            raise

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the ClickHouse.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()
            cur = connection.cursor()
            try:
                cur.execute('select 1;')
            finally:
                cur.close()
            response.success = True
        except SQLAlchemyError as e:
            logger.error(f'Error connecting to ClickHouse {self.connection_data["database"]}, {e}!')
            response.error_message = str(e)
            self.is_connected = False

        if response.success is True and need_to_close:
            self.disconnect()

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        connection = self.connect()
        cur = connection.cursor()
        try:
            cur.execute(query)
            result = cur.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pd.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description]
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            connection.commit()
        except SQLAlchemyError as e:
            logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            connection.rollback()
        finally:
            cur.close()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in ClickHouse db
        """
        q = f"SHOW TABLES FROM {self.connection_data['database']}"
        result = self.native_query(q)
        df = result.data_frame

        if df is not None:
            result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})

        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name}"
        result = self.native_query(q)
        return result
