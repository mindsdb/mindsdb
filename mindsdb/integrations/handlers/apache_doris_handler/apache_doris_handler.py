from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class ApacheDorisHandler(MySQLHandler):
    """This handler handles connection and execution of the Apache Doris statements."""

    name = 'apache_doris'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
