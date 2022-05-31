from contextlib import closing

import pymssql
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
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
    type = 'mssql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs
        self.dialect = 'mssql'
        self.database = kwargs.get('database')

    def __connect(self):
        """
        Handles the connection to a SQL Server insance.
        """
        connection = pymssql.connect(**self.connection_args)
        return connection

    def check_status(self) -> StatusResponse:
        """
        Check the connection of the SQL Server database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        try:
            con = self.__connect()
            with closing(con) as con:
                # TODO: best way to check con.connected ?
                response.success = True
        except Exception as e:
            log.error(f'Error connecting to SQL Server {self.database}, {e}!')
            response.error_message = str(e)
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in SQL Server
        :return: returns the records from the current recordset
        """
        con = self.__connect()
        with closing(con) as con:
            with con.cursor(as_dict=True) as cur:
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
