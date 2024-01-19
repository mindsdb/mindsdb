"""
This is the MySQL integration handler for mindsdb.  It provides the routines
which provide for interacting with the database.

MindsDB currently does not appear to multiple round trip transactions. This
makes sense given the niche that the project fulfills.  If this changes, most
handlers will require modification.  Here we would need a context manager for
handling transactions.  This would be safer than explicit commits since errors
would result in rolling back automatically.
"""

from collections import OrderedDict

import pandas as pd
import mysql.connector
from urllib.parse import urlparse
from sqlalchemy import create_engine

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

logger = log.getLogger(__name__)

class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = 'mysql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.mysql_url = None
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
        Unpacks the config from the connection_data.

        The connection_data must include either the old style of dictionary
        attriutes (host, optional port, user, password, database) OR have
        a url connection string with username and password optionally supplied
        in the dictionary itself.

        Arguments:
        - conection_data is the dictionary parsed from the JSON payload

        Exceptions thrown:
        - ValueError if data validation rules fail with a description as the sole
        argument

        Returns a dictionary with the relevant config info:
        - host
        - port
        - user
        - password
        - database
        """
        url = self.connection_data.get('url')
        if url:
            urlfields = urlparse(url)
            if urlfields.scheme != 'mysql':
                raise ValueError(
                      "If using a URL to connect to MySQL, the URL needs to start with 'mysql://'"
                )
            if urlfields.username and self.connection_data.get('user'):
                raise ValueError(
                      "Cannot specify a user in both the URL and elsewhere"
                )
            if urlfields.username and self.connection_data.get('password'):
                raise ValueError(
                      "Cannot specify a password in both the URL and elsewhere"
                )
            if not urlfields.host:
                raise ValueError(
                      "Connection URL does not include hostname"
                )
            if not urlfields.path:
                raise ValueError(
                      "Connection URL does not include database"
                )
            config = {
                'host'    : urlfields.host,
                'port'    : urlfields.port or  3306,
                'user'    : urlfields.username or self.connection_data.get('user'),
                'password': urlfields.password or self.connection_data.get('password'),
                'database': urlfields.path,
            }

        else:
            config = {
                'host': self.connection_data.get('host'),
                'port': self.connection_data.get('port') or 3306,
                'user': self.connection_data.get('user'),
                'password': self.connection_data.get('password'),
                'database': self.connection_data.get('database')
            }

            if not config.get('host'):
                raise ValueError("Must supply a host")
            if not config.get('database'):
                raise ValueError("Must supply a database name")

        if not config.get('user'):
            raise ValueError('Must supply a user')
        if not config.get('password'):
            raise ValueError('Must supply a password for connections')
        return config

    def connect(self):
        if self.is_connected is True:
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

        connection = mysql.connector.connect(**config)
        connection.autocommit = True
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the MySQL database
        :return: success status and error message if error occurs
        """

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection.is_connected()
        except Exception as e:
            logger.error(f'Error connecting to MySQL {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor(dictionary=True, buffered=True) as cur:
            try:
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
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
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
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
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
        'description': 'Set it to False to disable ssl.',
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
