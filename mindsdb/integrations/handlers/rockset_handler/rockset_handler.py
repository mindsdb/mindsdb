from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class RocksetHandler(MySQLHandler):
    """
    This handler handles connection and execution of the Rockset integration
    """
    name = 'rockset'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
