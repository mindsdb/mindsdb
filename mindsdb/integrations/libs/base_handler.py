from typing import Optional, Any

import pandas as pd
from mindsdb_sql.parser.ast import Join
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse


class BaseHandler:
    """ Base class for handlers

    Base class for handlers that associate a source of information with the
    broader MindsDB ecosystem via SQL commands.
    """

    def __init__(self, name: str):
        """ constructor
        Args:
            name (str): the handler name
        """
        self.is_connected: bool = False
        self.name = name

    def connect(self) -> HandlerStatusResponse:
        """ Set up any connections required by the handler

        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.

        Returns:
            HandlerStatusResponse
        """
        raise NotImplementedError()

    def disconnect(self):
        """ Close any existing connections

        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def check_connection(self) -> HandlerStatusResponse:
        """ Cehck connection to the handler

        Returns:
            HandlerStatusResponse
        """
        raise NotImplementedError()

    def native_query(self, query: Any) -> HandlerResponse:
        """Receive raw query and act upon it somehow.

        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def query(self, query: ASTNode) -> HandlerResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc

        Returns:
            HandlerResponse
        """
        raise NotImplementedError()

    def get_tables(self) -> HandlerResponse:
        """ Return list of entities

        Return list of entities that will be accesible as tables.

        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        raise NotImplementedError()

    def get_columns(self, table_name: str) -> HandlerResponse:
        """ Returns a list of entity columns

        Args:
            table_name (str): name of one of tables returned by self.get_tables()

        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
        raise NotImplementedError()


class DatabaseHandler(BaseHandler):
    """
    Base class for handlers associated to data storage systems (e.g. databases, data warehouses, streaming services, etc.)
    """
    def __init__(self, name: str):
        super().__init__(name)


class PredictiveHandler(BaseHandler):
    """
    Base class for handlers associated to predictive systems.
    """
    def __init__(self, name: str):
        super().__init__(name)

    def join(self, stmt, data_handler, into: Optional[str]) -> pd.DataFrame:
        """
        Join the output of some entity in the handler with output from some other handler.

        Data from the external handler should be retrieved via the `select_query` method.

        `into`: if provided, the resulting output will be stored in the specified data handler table via `handler.select_into()`. 
        """
        raise NotImplementedError()

    def _get_model_name(self, stmt):
        """ Discern between joined entities to retrieve model name, alias and the clause side it is on. """
        side = None
        models = self.get_tables().data_frame['model_name'].values
        if type(stmt.from_table) == Join:
            model_name = stmt.from_table.right.parts[-1]
            side = 'right'
            if model_name not in models:
                model_name = stmt.from_table.left.parts[-1]
                side = 'left'
            alias = str(getattr(stmt.from_table, side).alias)
        else:
            model_name = stmt.from_table.parts[-1]
            alias = None  # todo: fix this

        if model_name not in models:
            raise Exception("Error, not found. Please create this predictor first.")

        return model_name, alias, side
