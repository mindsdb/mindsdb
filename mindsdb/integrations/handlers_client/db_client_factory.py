import os
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class DBClientFactory:
    def __init__(self):
        self.host = os.environ.get("MINDSDB_DB_SERVICE_HOST", None)
        self.port = os.environ.get("MINDSDB_DB_SERVICE_PORT", None)
        if self.host is not None and self.port is not None:
            try:
                from mindsdb.integrations.handlers_client.db_grpc_client import DBClientGRPC
                self.client_class = DBClientGRPC
            except (ImportError, ModuleNotFoundError):
                logger.info("to work with microservice mode please install 'pip install mindsdb[grpc]'")
                self.host = None
                self.port = None

    def __call__(self, handler_type: str, handler: type, **kwargs: dict):
        if self.host is None or self.port is None:
            logger.info(
                "%s.__call__: no post/port to DBService have provided. Handle all db request locally",
                self.__class__.__name__,
            )
            return handler(**kwargs)

        logger.info("%s.__call__: api to communicate with db services - gRPC, host - %s, port - %s",
                    self.__class__.__name__,
                    self.host,
                    self.port,
                    )

        return self.client_class(handler_type, **kwargs)


DBClient = DBClientFactory()
