from collections import OrderedDict

import pandas as pd
import clickhouse_driver
from sqlalchemy import create_engine
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
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
        Handles the connection to a ClickHouse
        """
        if self.is_connected is True:
            return self.connection

        protocol = "clickhouse+native" if self.protocol == 'native' else "clickhouse+http"
        host = self.connection_data['host']
        port = self.connection_data['port']
        user = self.connection_data['user']
        password = self.connection_data['password']
        database = self.connection_data['database']
        url = f'{protocol}://{user}:{password}@{host}:{port}/{database}'
        if self.protocol == 'https':
            url = url + "?protocol=https"

        engine = create_engine(url)
        connection = engine.raw_connection()
        self.is_connected = True
        self.connection = connection
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the ClickHouse database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            cur = connection.cursor()
            try:
                cur.execute('select 1;')
            finally:
                cur.close()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to ClickHouse {self.connection_data["database"]}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in ClickHouse
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

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
        except Exception as e:
            log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            connection.rollback()
        finally:
            cur.close()

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
        Get a list with all of the tabels in ClickHouse db
        """
        q = f"SHOW TABLES FROM {self.connection_data['database']}"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name}"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
    protocol={
        'type': ARG_TYPE.STR,
        'protocol': 'The protocol to query clickhouse. Supported: native, http, https. Default: native'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the ClickHouse server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the ClickHouse server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the ClickHouse server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the ClickHouse server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the ClickHouse server. Must be an integer.'
    }
)

connection_args_example = OrderedDict(
    protocol='native',
    host='127.0.0.1',
    port=9000,
    user='root',
    password='password',
    database='database'
)
