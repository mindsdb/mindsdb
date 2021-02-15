from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import initialize_log


def start(verbose=False):
    config = Config()
    if verbose:
        config.set(['log', 'level', 'console'], 'DEBUG')

    initialize_log(config, 'mysql', wrap_print=True)

    MysqlProxy.startProxy(config)
