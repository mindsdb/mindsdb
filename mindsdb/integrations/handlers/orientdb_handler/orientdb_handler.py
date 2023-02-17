from collections import OrderedDict

import pandas as pd
import pyorient
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


class OrientDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the OrientDB statements.
    """

    name = 'orientdb'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'orientdb'
        self.connection_data = kwargs.get('connection_data', {})
        self.database = self.connection_data.get('database')
        self.connection = None
        self.is_connected = False

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

        # 连接
        connection = pyorient.OrientDB(config.get('host'), config.get('port'))
        connection.connect(config.get('user'), config.get('password'))
        connection.db_open(config.get('database'), config.get('user'), config.get('password'))
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
        Check the connection of the OrientDB database
        :return: success status and error message if error occurs
        """

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            result.success = self.is_connected
        except Exception as e:
            log.logger.error(f'Error connecting to OrientDB {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False
        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in OrientDB
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        tx = connection.tx_commit()

        # tx.begin()
        try:
            res = connection.command(query)
            if(len(res) != 0):

                """
                pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),          
                            columns = ['a', 'b', 'c']
                )
                """

                r = []
                for i in range(0, len(res)):
                    o = list((res[i].oRecordData.values()))
                    r.append(o)
                columns = list(res[0].oRecordData.keys())
                df = pd.DataFrame(r, columns=columns)

                response = Response(
                    RESPONSE_TYPE.TABLE,
                    df
                )
            else:
                response  = Response(RESPONSE_TYPE.OK)
            # tx.commit()

        except Exception as e:
            log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            # tx.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Get a list with all of the classes in OrientDB
        """
        q = "SELECT name as table_name FROM (SELECT expand(classes) FROM metadata:schema)"
        result = self.native_query(q)
        # df = result.data_frame
        # result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    # #
    # def get_columns(self, table_name) -> Response:
    #     """
    #     Show details about the table
    #     """
    #     q = f"DESC {table_name}"
    #     result = self.native_query(q)
    #     return result


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the OrientDB server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the OrientDB server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the OrientDB server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the OrientDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the OrientDB server. Must be an integer.'
    }

)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=2424,
    user='root',
    password='password',
    database='database'
)
