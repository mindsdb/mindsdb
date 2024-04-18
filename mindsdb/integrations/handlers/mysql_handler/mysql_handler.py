from collections import OrderedDict

import pandas as pd
import mysql.connector

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.handlers.mysql_handler.settings import ConnectionConfig

logger = log.getLogger(__name__)


class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = 'mysql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.connection_data = kwargs.get('connection_data', {})
        self.database = self.connection_data.get('database')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _unpack_config(self):
        """
        Unpacks the config from the connection_data by validation all parameters.

        Returns:
            dict: A dictionary containing the validated connection parameters.
        """
        try:
            config = ConnectionConfig(**self.connection_data)
            return config.dict(exclude_unset=True)
        except ValueError as e:
            raise ValueError(str(e))

    def connect(self):
        """
        Establishes a connection to a MySQL database.

        Returns:
            MySQLConnection: An active connection to the database.
        """
        if self.is_connected:
            return self.connection
        config = self._unpack_config()
        if 'conn_attrs' in self.connection_data:
            config['conn_attrs'] = self.connection_data['conn_attrs']

        ssl = self.connection_data.get('ssl')
        if ssl is True:
            ssl_ca = self.connection_data.get('ssl_ca')
            ssl_cert = self.connection_data.get('ssl_cert')
            ssl_key = self.connection_data.get('ssl_key')
            config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
            if ssl_ca is not None:
                config["ssl_ca"] = ssl_ca
            if ssl_cert is not None:
                config["ssl_cert"] = ssl_cert
            if ssl_key is not None:
                config["ssl_key"] = ssl_key
        try:
            connection = mysql.connector.connect(**config)
            connection.autocommit = True
            self.connection = connection
            self.is_connected = True
            return self.connection
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL {self.database}, {e}!")
            self.is_connected = False
            raise

    def disconnect(self):
        """
        Closes the connection to the MySQL database if it's currently open.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the MySQL database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """

        result = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            with self.connect() as connection:
                result.success = connection.is_connected()
        except mysql.connector.Error as e:
            logger.error(f'Error connecting to MySQL {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success and need_to_close:
            self.disconnect()
        if not result.success and self.is_connected:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query on the MySQL database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        need_to_close = not self.is_connected
        try:
            with self.connect() as connection:
                with connection.cursor(dictionary=True, buffered=True) as cur:
                    cur.execute(query)
                    if cur.with_rows:
                        result = cur.fetchall()
                        response = Response(
                            RESPONSE_TYPE.TABLE,
                            pd.DataFrame(
                                result,
                                columns=[x[0] for x in cur.description]
                            )
                        )
                    else:
                        response = Response(RESPONSE_TYPE.OK)
        except mysql.connector.Error as e:
            logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            if connection.is_connected():
                connection.rollback()

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL selected database
        """
        sql = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """
        result = self.native_query(sql)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE `{table_name}`;"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
    url={
        'type': ARG_TYPE.STR,
        'description': 'The URI-Like connection string to the MySQL server. If provided, it will override the other connection arguments.',
        'required': False,
        'label': 'URL'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the MySQL server.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the MySQL server.',
        'required': True,
        'label': 'Password'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the MySQL server.',
        'required': True,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the MySQL server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MySQL server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'Set it to True to enable ssl.',
        'required': False,
        'label': 'ssl'
    },
    ssl_ca={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file',
        'required': False,
        'label': 'ssl_ca'
    },
    ssl_cert={
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file',
        'required': False,
        'label': 'ssl_cert'
    },
    ssl_key={
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file',
        'required': False,
        'label': 'ssl_key',
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='password',
    database='database'
)
