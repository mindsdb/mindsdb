import mindsdb.interfaces.storage.db as db
from mindsdb.api.postgres.postgres_proxy.postgres_proxy import PostgresProxyHandler
from mindsdb.utilities import log


def start(verbose=False):
    logger = log.getLogger(__name__)
    logger.info("Postgres API is starting..")
    db.init()

    PostgresProxyHandler.startProxy()
