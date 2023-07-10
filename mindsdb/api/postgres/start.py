import logging

import mindsdb.interfaces.storage.db as db
from mindsdb.api.postgres.postgres_proxy.postgres_proxy import PostgresProxyHandler
from mindsdb.utilities import log


def start(verbose=False):
    log.configure_logging()  # Because this is the entrypoint for a process, we need to config logging
    logger = logging.getLogger(__name__)
    logger.info("Postgres API is starting..")
    db.init()

    PostgresProxyHandler.startProxy()
