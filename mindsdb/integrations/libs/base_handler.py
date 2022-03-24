from typing import Dict, List, Optional

class BaseHandler:
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

    def select_query(self, stmt) -> pd.DataFrame:
        """
        Select data from some entity in the handler and return in dataframe format.
        
        This method assumes the raw_query has been parsed with mindsdb_sql using some dialect compatible with the handler, and so the statement object `stmt` has all information we need.
        """  # noqa
        raise NotImplementedError()

    def join(self, stmt, data_handler: BaseHandler) -> pd.DataFrame:
        """
        Join the output of some entity in the handler with output from some other handler.
        
        Data from the external handler should be retrieved via the `select_query` method.
        """  # noqa
        raise NotImplementedError()


class DatabaseHandler(BaseHandler):
    def __init__(self, name):
        super().__init__(name)

    def get_views(self):
        raise NotImplementedError()

    def select_into(self, integration_instance, stmt):
        raise NotImplementedError()


class PredictiveHandler(BaseHandler):
    def __init__(self, name):
        super().__init__(name)




























