from ..mysql_handler import Handler as MySQLHandler


class PlanetScaleHandler(MySQLHandler):
    """
    This handler handles connection and execution of queries against PlanetScale.
    """
    name = 'planet_scale'
