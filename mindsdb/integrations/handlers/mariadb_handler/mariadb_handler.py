from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class MariaDBHandler(MySQLHandler):
    """
    This handler handles connection and execution of the MariaDB statements.
    """

    name = 'mariadb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
