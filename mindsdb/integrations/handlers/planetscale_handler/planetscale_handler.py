from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler


class PlanetScaleHandler(MySQLHandler):
    """
    This handler handles the connection and execution of queries against PlanetScale.
    """
    name = 'planet_scale'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
