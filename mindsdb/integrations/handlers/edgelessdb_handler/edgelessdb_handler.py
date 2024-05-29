from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class EdgelessDBHandler(MySQLHandler):
    """
    This handler handles connection and execution of the EdgelessDB statements.
    """

    name = 'edgelessdb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
