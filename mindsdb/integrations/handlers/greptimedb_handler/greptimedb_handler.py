"""
This is the GreptimeDB integration handler for mindsdb.  It provides the routines
which provide for interacting with the database.

Because GreptimeDB has built-in MySQL wire protocol support, this handler is simply
 a subclass of mindsdb's MySQL handler
"""

from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class GreptimeDBHandler(MySQLHandler):
    """
    This handler handles connection and execution of GreptimeDB statements.
    It's a subclass of default mysql handler
    """

    name = 'greptimedb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
