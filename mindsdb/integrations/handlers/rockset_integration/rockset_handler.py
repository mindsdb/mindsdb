from ..mysql_handler import Handler as MySQLHandler, connection_args, connection_args_example

class RocksetIntegration(MySQLHandler):
    """
    This handler handles connection and execution of the Rockset integration
    """
    name = 'rockset'
