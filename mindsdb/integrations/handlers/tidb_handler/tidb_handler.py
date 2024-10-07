from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class TiDBHandler(MySQLHandler):
    """
    This handler handles connection and execution of the TiDB statements.
    """
    name = 'tidb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
