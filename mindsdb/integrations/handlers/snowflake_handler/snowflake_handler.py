from pandas import DataFrame
from snowflake import connector
from snowflake.sqlalchemy import snowdialect

from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


class SnowflakeHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Snowflake statements.
    """

    name = 'snowflake'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get('connection_data')
        self.is_connected = False
        self.connection = None

    def connect(self):
        if self.is_connected is True:
            return self.connection
        con = connector.connect(
            host=self.connection_data['host'],
            user=self.connection_data['user'],
            password=self.connection_data['password'],
            account=self.connection_data['account'],
            warehouse=self.connection_data['warehouse'],
            database=self.connection_data['database'],
            schema=self.connection_data['schema'],
            protocol=self.connection_data['protocol'],
            port=self.connection_data['port'],
            application='MindsDB'
        )
        self.is_connected = True
        self.connection = con
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return 

    def check_connection(self) -> StatusResponse:
        """
        Check the connection
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False
        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('select 1;')
            response.success = True
        except connector.errors.ProgrammingError as e:
            log.logger.error(f'Error connecting to Snowflake {self.connection_data["database"]}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()

        if response.success is False and self.is_connected is True:
            self.is_connected = False
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Snowflake
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False
        connection = self.connect()
        from snowflake.connector import DictCursor
        with connection.cursor(DictCursor) as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
        if need_to_close is True:
            self.disconnect()
        return response

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in the current database or schema
        that the user has acces to
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        return result

    def get_columns(self, table_name) -> Response:
        """
        List the columns in the tabels for which the user have access
        """
        q = f"SHOW COLUMNS IN TABLE {table_name};"
        result = self.native_query(q)
        return result

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender(snowdialect.dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)
