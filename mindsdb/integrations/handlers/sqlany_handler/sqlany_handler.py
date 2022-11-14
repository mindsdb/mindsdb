from textwrap import dedent
from collections import OrderedDict

from pandas import DataFrame

import sqlanydb
import sqlalchemy_sqlany.base as sqlany_dialect

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


log = get_log()


class SQLAnyHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SAP SQL Anywhere statements.
    """

    name = 'sqlany'

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)

        self.dialect = 'sqlany'
        self.parser = parse_sql
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(sqlany_dialect.SQLAnyDialect)
        self.host = self.connection_data.get('host')
        self.port = self.connection_data.get('port')
        self.userid = self.connection_data.get('user')
        self.password = self.connection_data.get('password')
        self.server = self.connection_data.get('server')
        self.databaseName = self.connection_data.get('database')
        self.encryption = self.connection_data.get('encrypt', False)
        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a SAP SQL Anywhere database insance.
        """

        if self.is_connected is True:
            return self.connection

        if self.port.strip().isnumeric():
            self.host += ":" + self.port.strip()

        if self.encryption:
            self.encryption = "SIMPLE"
        else:
            self.encryption = "NONE"
        
        connection = sqlanydb.connect(
            host=self.host,
            userid=self.userid,
            password=self.password,
            server=self.server,
            databaseName=self.databaseName,
            encryption=self.encryption
        )
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        """
        Disconnects from the SAP SQL Anywhere database
        """

        if self.is_connected is True:
            self.connection.close()
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the SAP SQL Anywhere database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            cur = connection.cursor()
            cur.execute('SELECT 1 FROM SYS.DUMMY;')
            response.success = True
        except sqlanydb.Error as e:
            log.error(f'Error connecting to SAP SQL Anywhere {self.host}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in SAP SQL Anywhere
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        cur = connection.cursor()
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
            log.error(f'Error running query: {query} on {self.connection}!')
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

        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in SAP SQL Anywhere in the current schema
        """

        return self.native_query(f"""
            SELECT USER_NAME(ob.UID) AS SCHEMA_NAME
            , st.TABLE_NAME
            , st.TABLE_TYPE
            FROM SYSOBJECTS ob
            INNER JOIN SYS.SYSTABLE st on ob.ID = st.OBJECT_ID
            WHERE ob.TYPE='U' AND st.TABLE_TYPE <> 'GBL TEMP'
        """)

    def get_columns(self, table_name: str) -> Response:
        """
        List all columns in a table in SAP SQL Anywhere in the current schema
        :param table_name: the table name for which to list the columns
        :return: returns the columns in the table
        """

        return self.renderer.dialect.get_columns(table_name)

# For complete list of parameters: https://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.help.sqlanywhere.12.0.1/dbadmin/da-conparm.html
connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the SAP SQL Anywhere instance host.'
    },
    port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the SAP SQL Anywhere instance.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the user name.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the password for the user.'
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the server to connect to.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the database to connect to.'
    },
    encrypt={
        'type': ARG_TYPE.BOOL,
        'description': 'Enables or disables TLS encryption.'
    },
)

connection_args_example = OrderedDict(
    host='localhost',
    port=55505,
    user='DBADMIN',
    password='password',
    serverName='TestMe',
    database='MINDSDB'
)
