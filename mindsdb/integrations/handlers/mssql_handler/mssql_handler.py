from contextlib import closing

import pymssql
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class SqlServerHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Microsoft SQL Server statements. 
    """
    name = 'mssql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs
        self.connection_data = self.connection_args.get('connection_data')
        self.dialect = 'mssql'
        self.database = kwargs.get('database')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a SQL Server insance.
        """
        if self.is_connected is True:
            return self.connection

        self.connection = pymssql.connect(**self.connection_data)
        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the SQL Server database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to SQL Server {self.database}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in SQL Server
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        # with closing(connection) as con:
        with connection.cursor(as_dict=True) as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.database}!')
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
        renderer = SqlalchemyRender('mssql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL
        """
        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW');
        """
        result = self.native_query(query)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        result = self.native_query(q)
        return result
