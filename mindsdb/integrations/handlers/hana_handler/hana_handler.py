from pandas import DataFrame

from hdbcli import dbapi
import sqlalchemy_hana.dialect as hana_dialect

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


logger = log.getLogger(__name__)

class HanaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SAP Hana statements.
    """

    name = 'hana'

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)

        self.connection_data = connection_data

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a SAP Hana database insance.
        """

        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['address', 'port', 'user', 'password']):
            raise ValueError('Required parameters (address, port, user, password) must be provided.')
        
        config = {
            'address': self.address,
            'port': self.port,
            'user': self.user,
            'password': self.password,
        }

        # Optional connection parameters.
        if 'database' in self.connection_data:
            config['databaseName'] = self.databaseName

        if 'schema' in self.connection_data:
            config['currentSchema'] = self.currentSchema

        self.connection = dbapi.connect(
            **config
        )

        self.is_connected = True
        return self.connection

    def disconnect(self):
        """
        Disconnects from the SAP HANA database
        """

        if self.is_connected is True:
            self.connection.close()
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the SAP HANA database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('SELECT 1 FROM SYS.DUMMY')
            response.success = True
        except dbapi.Error as e:
            logger.error(f'Error connecting to SAP HANA {self.address}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in SAP HANA
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if not cur.description:
                    response = Response(RESPONSE_TYPE.OK)
                else:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.address}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        renderer = SqlalchemyRender(hana_dialect.HANAHDBCLIDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in SAP HANA in the current schema
        """

        query = """
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   'BASE TABLE' AS TABLE_TYPE
            FROM
                SYS.TABLES
            WHERE IS_SYSTEM_TABLE = 'FALSE'  
              AND IS_USER_DEFINED_TYPE = 'FALSE'
              AND IS_TEMPORARY = 'FALSE'

            UNION

            SELECT SCHEMA_NAME,
                   VIEW_NAME AS TABLE_NAME,
                   'VIEW' AS TABLE_TYPE
            FROM
                SYS.VIEWS
            WHERE SCHEMA_NAME <> 'SYS'
              AND SCHEMA_NAME NOT LIKE '_SYS%'            
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
        """
        List all columns in a table in SAP HANA in the current schema
        :param table_name: the table name for which to list the columns
        :return: returns the columns in the table
        """

        return self.renderer.dialect.get_columns(table_name)
