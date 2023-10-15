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


class ApacheDorisHandler(DatabaseHandler):
    """This handler handles connection and execution of the Apache Doris statements."""

    name = 'apache_doris'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.connection_data = kwargs.get('connection_data', {})
        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        if self.is_connected is True:
            return self.connection
        config = {
            'host': self.connection_data.get('host'),
            'port': self.connection_data.get('port'),
            'user': self.connection_data.get('user'),
            'password': self.connection_data.get('password'),
            'database': self.connection_data.get('database')
        }
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
        result = StatusResponse(False)
        need_to_close = self.is_connected is False
        try:
            connection = self.connect()
            result.success = connection.is_connected()
        except Exception as e:
            log.logger.error(f'Error connecting to Apache Doris database {self.connection_data["database"]}: {e}!')
            result.error_message = str(e)
        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False
        return result

    def native_query(self, query: str) -> Response:
        need_to_close = self.is_connected is False
        if not self.connection:
            self.connection = self.connect()
        with self.connection.cursor(dictionary=True, buffered=True) as cur:
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
                self.connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()
        if need_to_close is True:
            self.disconnect()
        return response

    def query(self, query: ASTNode) -> Response:
        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
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
            ORDER BY 2;
        """
        result = self.native_query(sql)
        return result

    def get_columns(self, table_name) -> Response:
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Apache Doris server',
        'required': True,
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'In integer representing the TCP/IP port of the Apache Doris frontend server',
        'required': True,
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Apache Doris server',
        'required': True,
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Apache Doris server',
        'required': True,
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Apache Doris server',
        'required': True,
    },
    ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'Whether to enable SSL connection',
        'required': False,
    },
    ssl_ca={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file',
        'required': False,
    },
    ssl_cert={
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file',
        'required': False,
    },
    ssl_key={
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file',
        'required': False,
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='password',
    database='database'
)
