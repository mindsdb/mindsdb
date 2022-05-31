from typing import Dict, List, Optional, Any

import pandas as pd
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse


class BaseHandler:
    """
    Base class for handlers that associate a source of information with the broader MindsDB ecosystem via SQL commands.
    """
    def __init__(self, name):
        self.name = name

    def connect(self, **kwargs) -> Dict[str, int]:
        """
        Set up any connections required by the handler here.

        Should return output of check_status() method after attempting connection.
        """
        raise NotImplementedError()

    def check_status(self) -> HandlerStatusResponse:
        """ Heartbeat method, should return 200 OK or 503 if there is an issue with the handler. """
        raise NotImplementedError()

    def native_query(self, query: Any) -> HandlerResponse:
        """
        Receive raw query and act upon it somehow.

        Args:
            query (Any): query in native format (str for sql databases, dict for mongo, etc)

        Returns:
            dict with response:
            {
                'type': 'table'
                'data_frame': DataFrame
            }
            {
                'type': 'ok'
            }
            {
                'type': 'error',
                'error_code': int,
                'error_message': str
            }
        """
        raise NotImplementedError()

    def query(self, query: ASTNode) -> HandlerResponse:
        """
        Select data from some entity in the handler and return in dataframe format.

        This method parses a raw query with SqlalchemyRender using specific dialect compatible with the handler.
        """
        raise NotImplementedError()

    def get_tables(self) -> HandlerResponse:
        """
        Return list of entities that will be accesible as tables thanks to the handler. 
        """
        raise NotImplementedError()

    def get_columns(self, table_name: str) -> HandlerResponse:
        """
        For getting standard info about a table. e.g. data types
        """
        raise NotImplementedError()


class DatabaseHandler(BaseHandler):
    """
    Base class for handlers associated to data storage systems (e.g. databases, data warehouses, streaming services, etc.)
    """
    def __init__(self, name):
        super().__init__(name)


class PredictiveHandler(BaseHandler):
    """
    Base class for handlers associated to predictive systems.
    """
    def __init__(self, name):
        super().__init__(name)

    def join(self, stmt, data_handler, into: Optional[str]) -> pd.DataFrame:
        """
        Join the output of some entity in the handler with output from some other handler.

        Data from the external handler should be retrieved via the `select_query` method.

        `into`: if provided, the resulting output will be stored in the specified data handler table via `handler.select_into()`. 
        """
        raise NotImplementedError()
