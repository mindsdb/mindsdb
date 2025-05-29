from typing import Optional

from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler


class CloudSQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Google Cloud SQL statements.
    """
    name = 'cloud_sql'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        self.dialect = 'cloud_sql'
        self.connection_data = connection_data
        self.kwargs = kwargs

        if self.connection_data['db_engine'] == 'mysql':
            self.db = MySQLHandler(
                name=name + 'mysql',
                connection_data={key: self.connection_data[key] for key in self.connection_data if key != 'db_engine'}
            )
        elif self.connection_data['db_engine'] == 'postgresql':
            self.db = PostgresHandler(
                name=name + 'postgresql',
                connection_data={key: self.connection_data[key] for key in self.connection_data if key != 'db_engine'}
            )
        elif self.connection_data['db_engine'] == 'mssql':
            self.db = SqlServerHandler(
                name=name + 'mssql',
                connection_data={key: self.connection_data[key] for key in self.connection_data if key != 'db_engine'}
            )
        else:
            raise Exception("The database engine should be either MySQL, PostgreSQL or SQL Server!")

    def __del__(self):
        self.db.__del__()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        return self.db.connect()

    def disconnect(self):
        """
        Close any existing connections.
        """

        return self.db.disconnect()

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        return self.db.check_connection()

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        return self.db.native_query(query)

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.db.query(query)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        return self.db.get_tables()

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        return self.db.get_columns(table_name)
