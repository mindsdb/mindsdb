from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb.utilities.functions import init_lexer_parsers


def start(verbose=False):
    db.init()
    init_lexer_parsers()

    config = Config()

    log.initialize_log(config, 'mysql', wrap_print=True)

    MysqlProxy.startProxy()
