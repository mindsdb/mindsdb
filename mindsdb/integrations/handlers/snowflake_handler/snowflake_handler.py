
from snowflake import connector
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from pandas import DataFrame
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.utilities.log import log
from mindsdb_sql.parser.ast.base import ASTNode


class SnowflakeHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Snowflake statements.
    """

    name = 'snowflake'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.host = kwargs.get('host')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.account = kwargs.get('account')
        self.warehouse = kwargs.get('warehouse')
        self.database = kwargs.get('database')
        self.schema = kwargs.get('schema')
        self.protocol = kwargs.get('protocol')
        self.port = kwargs.get('port')
        self.is_connected = False
        self.connection = None

    def connect(self):
        if self.is_connected is True:
            return self.connection

        con = connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            protocol=self.protocol,
            port=self.port,
            application='MindsDB'
        )

        self.is_connected = True
        self.connection = con
        return con

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

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('select 1;')
            response.success = True
        except connector.errors.ProgrammingError as e:
            log.error(f'Error connecting to Snowflake {self.database}, {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Snowflake
        :return: returns the records from the current recordset
        """

        connection = self.connect()
        with connection.cursor() as cur:
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
                log.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
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
        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)
