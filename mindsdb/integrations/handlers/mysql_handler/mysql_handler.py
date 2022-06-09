from contextlib import closing

import pandas as pd
import mysql.connector

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    type = 'mysql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.connection_data = kwargs.get('connection_data')

    def connect(self):
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
            config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
            if ssl_ca is not None:
                config["ssl_ca"] = ssl_ca
            if ssl_cert is not None:
                config["ssl_cert"] = ssl_cert
            if ssl_key is not None:
                config["ssl_key"] = ssl_key

        connection = mysql.connector.connect(**config)
        return connection

    def check_status(self) -> StatusResponse:
        """
        Check the connection of the MySQL database
        :return: success status and error message if error occurs
        """

        result = StatusResponse(False)
        try:
            con = self.connect()
            with closing(con) as con:
                result.success = con.is_connected()
        except Exception as e:
            log.error(f'Error connecting to MySQL {self.database}, {e}!')
            result.error_message = str(e)
        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """
        con = self.connect()
        with closing(con) as con:
            with con.cursor(dictionary=True, buffered=True) as cur:
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
                    log.error(f'Error running query: {query} on {self.database}!')
                    response = Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=str(e)
                    )
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
        Get a list with all of the tabels in MySQL
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result
