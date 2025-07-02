from typing import Optional

import pandas as pd
import pymysql as matone
from pymysql.cursors import DictCursor as dict
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


logger = log.getLogger(__name__)


class MatrixOneHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MatrixOne statements.
    """

    name = 'matrixone'

    def __init__(self, name, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.connection_data = connection_data
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

        ssl = self.connection_data.get('ssl')
        if ssl is True:
            ssl_ca = self.connection_data.get('ssl_ca')
            ssl_cert = self.connection_data.get('ssl_cert')
            ssl_key = self.connection_data.get('ssl_key')
            config['client_flags'] = [matone.constants.ClientFlag.SSL]
            if ssl_ca is not None:
                config["ssl_ca"] = ssl_ca
            if ssl_cert is not None:
                config["ssl_cert"] = ssl_cert
            if ssl_key is not None:
                config["ssl_key"] = ssl_key

        connection = matone.connect(**config)
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
        Check the connection of the MatrixOne database
        :return: success status and error message if error occurs
        """

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection.open
        except Exception as e:
            logger.error(f'Error connecting to MatrixOne {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MatrixOne
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor(cursor=dict) as cur:
            try:
                cur.execute(query)
                if cur._rows:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        pd.DataFrame(
                            result,
                            # columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
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
        Get a list with all of the tabels in MatrixOne
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"SHOW COLUMNS FROM {table_name};"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={
            df.columns[0]: 'COLUMN_NAME',
            df.columns[1]: 'DATA TYPE'
        })

        return result
