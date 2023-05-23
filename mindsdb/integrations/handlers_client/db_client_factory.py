import os
from mindsdb.utilities.log import get_log
from mindsdb.integrations.handlers_client.db_grpc_client import DBClientGRPC


logger = get_log(logger_name="main")


class DBClientFactory:
    def __init__(self):
        self.client_class = DBClientGRPC
        self.host = os.environ.get("MINDSDB_DB_SERVICE_HOST", None)
        self.port = os.environ.get("MINDSDB_DB_SERVICE_PORT", None)

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
