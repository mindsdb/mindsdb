from mindsdb.api.postgres.postgres_proxy.postgres_proxy import PostgresProxyHandler
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities import log


def start(verbose=False):
    db.init()

    config = Config()

    log.initialize_log(config, 'postgres_proxy', wrap_print=True)

    PostgresProxyHandler.startProxy()