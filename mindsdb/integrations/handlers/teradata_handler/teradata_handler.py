from textwrap import dedent
from collections import OrderedDict

from pandas import DataFrame
from sqlalchemy import String

from sqlalchemy.sql import text, bindparam

import teradatasql
import teradatasqlalchemy.dialect as teradata_dialect

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class TeradataHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Teradata statements.
    """

    name = 'teradata'

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)

        self.dialect = 'teradata'
        self.parser = parse_sql
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(teradata_dialect.TeradataDialect)

        self.host = self.connection_data.get('host')
        self.database = self.connection_data.get('database')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a Teradata database insance.
        """

        if self.is_connected is True:
            return self.connection

        connection = teradatasql.connect(
            **self.connection_data
        )

        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        """
        Disconnects from the Teradata database
        """

        if self.is_connected is True:
            self.connection.close()
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Teradata database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('SELECT 1 FROM (SELECT 1 AS "dual") AS "dual"')
            response.success = True
        except teradatasql.Error as e:
            log.logger.error(f'Error connecting to Teradata {self.host}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Teradata
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
                log.logger.error(f'Error running query: {query} on {self.host}!')
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
        List all tables in Teradata in the current database
        """

        return self.native_query(
            str(text(f"""
            SELECT DataBaseName,
                   TableName,
                   TableKind
            FROM DBC.TablesV
            WHERE DatabaseName = :database
            AND (TableKind = 'T'
                OR TableKind = 'O'
                OR TableKind = 'Q')
            """).bindparams(
                bindparam('database', value=self.database, type_=String)
            ).compile(compile_kwargs={"literal_binds": True}))
        )

    def get_columns(self, table_name: str) -> Response:
        """
        List all columns in a table in Teradata in the current schema
        :param table_name: the table name for which to list the columns
        :return: returns the columns in the table
        """

        return self.native_query(
            str(text(f"""
            SELECT ColumnName AS "Field",
                   ColumnType AS "Type"
            FROM DBC.ColumnsV
            WHERE DatabaseName (NOT CASESPECIFIC) = :database
            AND TableName (NOT CASESPECIFIC) = :table_name
            """).bindparams(
                bindparam('database', value=self.database, type_=String),
                bindparam('table_name', value=table_name, type_=String)
            ).compile(compile_kwargs={"literal_binds": True}))
        )


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the Teradata instance host.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the user name.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the password for the user.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the initial database to use after logon, instead of the default database.'
    },
    dbs_port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the Teradata instance.'
    },
    encryptdata={
        'type': ARG_TYPE.STR,
        'description': 'Controls encryption of data exchanged between the driver and the database.'
    },
    https_port={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the database port number for HTTPS/TLS connections.'
    },
    sslca={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the file name of a PEM file that contains Certificate Authority (CA) certificates.'
    },
    sslcapath={
        'type': ARG_TYPE.STR,
        'description': 'Specifies a directory of PEM files that contain Certificate Authority (CA) certificates.'
    },
    sslcipher={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the TLS cipher for HTTPS/TLS connections.'
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the SSL mode for HTTPS/TLS connections.'
    },
    sslprotocol={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the TLS protocol for HTTPS/TLS connections.'
    },
    tmode={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the Teradata transaction mode.'
    },
    logmech={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the login authentication method.'
    },
    logdata={
        'type': ARG_TYPE.STR,
        'description': 'Specifies extra data for the chosen logon authentication method.'
    },
    browser={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the command to open the browser for Browser Authentication.'
    }
)

connection_args_example = OrderedDict(
    host='192.168.0.41',
    user='dbc',
    password='dbc',
    database='HR'
)
