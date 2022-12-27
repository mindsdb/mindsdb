from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities import log


def start(verbose=False):
    db.init()

    config = Config()

    log.initialize_log(config, 'mysql', wrap_print=True)

    MysqlProxy.startProxy()
