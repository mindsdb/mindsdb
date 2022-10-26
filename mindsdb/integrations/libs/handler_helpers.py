from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler


def define_ml_handler(_type):
    _type = _type.lower()
    if _type == 'lightwood':
        try:
            from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
            return LightwoodHandler
        except ImportError:
            pass
    elif _type == 'huggingface':
        try:
            from mindsdb.integrations.handlers.huggingface_handler.huggingface_handler import HuggingFaceHandler
            return HuggingFaceHandler
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
