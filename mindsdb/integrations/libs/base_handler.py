from typing import Dict, List, Optional
import pandas as pd

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

    def run_native_query(self, query_str: str) -> Optional[object]:
        """ 
        Receive raw SQL and act upon it somehow.
        """  # noqa
        raise NotImplementedError()

    def select_query(self, targets, from_stmt, where_stmt) -> pd.DataFrame:
        """
        Select data from some entity in the handler and return in dataframe format.
        
        This method assumes a raw query has been parsed beforehand with mindsdb_sql using some dialect compatible with the handler, and only targets, from, and where clauses are fed into it.
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
