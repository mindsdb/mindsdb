import time
from typing import Optional, List

import duckdb
from typing import Any

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse, HandlerStatusResponse
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender


class DummyHandler(DatabaseHandler):
    name = 'dummy_data'

    def __init__(self, **kwargs):
        super().__init__('dummy_data')
        self.db_path = None

        args = kwargs.get('connection_data', {})
        if 'db_path' in args:
            self.db_path = args['db_path']

    def connect(self):
        """Set up any connections required by the handler"""
        return self.db_path is not None

    def disconnect(self):
        """ Close any existing connections"""
        return

    def check_connection(self) -> HandlerStatusResponse:
        """Check connection to the handler

        Returns:
            HandlerStatusResponse
        """
        return HandlerStatusResponse(success=True)

    def native_query(self, query: Any, params: Optional[List] = None) -> HandlerResponse:
        """Receive raw query and act upon it somehow

        Args:
            query (Any): query in native format (str for sql databases, dict for mongo, etc)
            params (Optional[List])

        Returns:
            HandlerResponse
        """
        con = duckdb.connect(self.db_path)
        if params is not None:
            query = query.replace('%s', '?')
            cur = con.executemany(query, params)
            if cur.rowcount >= 0:
                result_df = cur.fetchdf()
            else:
                con.close()
                return HandlerResponse(RESPONSE_TYPE.OK)
        else:
            result_df = con.execute(query).fetchdf()
        con.close()
        return HandlerResponse(RESPONSE_TYPE.TABLE, result_df)

    def query(self, query: ASTNode) -> HandlerResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc

        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender('postgres')
        query_str, params = renderer.get_exec_params(query, with_failback=True)
        return self.native_query(query_str, params)

    def get_tables(self) -> HandlerResponse:
        """Get a list of all the tables in the database

        Returns:
            HandlerResponse: Names of the tables in the database
        """
        q = 'SHOW TABLES;'
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Get details about a table

        Args:
            table_name (str): Name of the table to retrieve details of.

        Returns:
            HandlerResponse: Details of the table.
        """
        query = f'DESCRIBE {table_name};'
        return self.native_query(query)

    def subscribe(self, stop_event, callback, table_name, columns=None, **kwargs):

        while True:
            if stop_event.is_set():
                return
            time.sleep(0.3)
