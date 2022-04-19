from typing import Dict, List, Optional, Any

import pandas as pd
from mindsdb_sql.parser.ast.base import ASTNode


class BaseHandler:
    """
    Base class for handlers that associate a source of information with the broader MindsDB ecosystem via SQL commands.
    """  # noqa
    def __init__(self, name):
        self.name = name

    def connect(self, **kwargs) -> Dict[str, int]:
        """ 
        Set up any connections required by the handler here.
        
        Should return output of check_status() method after attempting connection.
        """  # noqa
        raise NotImplementedError()

    def check_status(self) -> Dict[str, int]:
        """ Heartbeat method, should return 200 OK or 503 if there is an issue with the handler. """  # noqa
        raise NotImplementedError()

    def get_tables(self) -> List:
        """
        Return list of entities that will be accesible as tables thanks to the handler. 
        """  # noqa
        raise NotImplementedError()

    def describe_table(self, table_name: str) -> Dict:
        """
        For getting standard info about a table. e.g. data types
        """  # noqa
        raise NotImplementedError()

    def native_query(self, query: Any) -> dict:
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
        """  # noqa
        raise NotImplementedError()

    def query(self, query: ASTNode) -> dict:
        """
        Select data from some entity in the handler and return in dataframe format.
        
        This method parses a raw query with SqlalchemyRender using specific dialect compatible with the handler.
        """  # noqa
        raise NotImplementedError()

    def join(self, stmt, data_handler, into: Optional[str]) -> pd.DataFrame:
        """
        Join the output of some entity in the handler with output from some other handler.
        
        Data from the external handler should be retrieved via the `select_query` method.
        
        `into`: if provided, the resulting output will be stored in the specified data handler table via `handler.select_into()`. 
        """  # noqa
        raise NotImplementedError()


class DatabaseHandler(BaseHandler):
    """
    Base class for handlers associated to data storage systems (e.g. databases, data warehouses, streaming services, etc.)
    """  # noqa
    def __init__(self, name):
        super().__init__(name)

    def get_views(self) -> List:
        raise NotImplementedError()

    def select_into(self, table: str, dataframe: pd.DataFrame):
        raise NotImplementedError()


class PredictiveHandler(BaseHandler):
    """
    Base class for handlers associated to predictive systems.
    """  # noqa
    def __init__(self, name):
        super().__init__(name)
