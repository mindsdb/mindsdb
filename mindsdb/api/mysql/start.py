import logging

import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities import log
from mindsdb.utilities.functions import init_lexer_parsers


def start(verbose=False):
    log.configure_logging()  # Because this is the entrypoint for a process, we need to config logging
    logger = logging.getLogger(__name__)
    logger.info("MySQL API is starting..")
    db.init()
    init_lexer_parsers()

    MysqlProxy.startProxy()
