from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler
# from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler


def define_ml_handler(_type):
    _type = _type.lower()
    if _type == 'lightwood':
        try:
            from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
            return LightwoodHandler
        except ImportError:
            pass
    return None


def define_handler(_type):
    _type = _type.lower()
    if _type == 'mysql':
        return MySQLHandler
    if _type == 'postgres':
        return PostgresHandler
    if _type == 'mariadb':
        return MariaDBHandler
    return None
