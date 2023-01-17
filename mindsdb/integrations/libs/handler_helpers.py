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


def _clean_pdef(pdef):
    """ Simple helper method that removes backticks from columns that collide with special string in mindsdb_sql. """
    if 'target' in pdef and '`target`' == pdef['target']:
        pdef['target'] = pdef['target'].replace('`', '')

    if 'timeseries_settings' in pdef:
        if '`order`' == pdef['timeseries_settings']['order_by']:
            pdef['timeseries_settings']['order_by'] = 'order'

        if 'group_by' in pdef['timeseries_settings']:
            for i, gcol in enumerate(pdef['timeseries_settings']['group_by']):
                if '`group`' == gcol:
                    pdef['timeseries_settings']['group_by'][i] = 'group'

    return pdef
