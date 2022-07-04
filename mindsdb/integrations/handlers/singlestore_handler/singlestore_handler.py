from ..mysql_handler import Handler as MySQLHandler


class SingleStoreHandler(MySQLHandler):
    """
    This handler handles connection and execution of the SingleStore statements.
    """
    name = 'singlestore'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
