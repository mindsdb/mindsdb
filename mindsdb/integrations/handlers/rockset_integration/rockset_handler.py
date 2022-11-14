from ..mysql_handler import Handler as MySQLHandler, connection_args, connection_args_example

class RocksetIntegration(MySQLHandler):
    """
    This handler handles connection and execution of the Rockset integration
    """
    name = 'rockset'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

