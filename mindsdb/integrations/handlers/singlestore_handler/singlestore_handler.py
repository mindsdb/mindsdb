from ..mysql_handler import Handler as MySQLHandler


class SingleStoreHandler(MySQLHandler):
    """
    This handler handles connection and execution of the ingleStore statements.
    """
    name = 'singlestore'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)