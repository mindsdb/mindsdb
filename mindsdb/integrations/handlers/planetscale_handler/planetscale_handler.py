from ..mysql_handler import Handler as MySQLHandler, connection_args, connection_args_example


class PlanetScaleHandler(MySQLHandler):
    """
    This handler handles connection and execution of queries against PlanetScale.
    """
    name = 'planet_scale'
