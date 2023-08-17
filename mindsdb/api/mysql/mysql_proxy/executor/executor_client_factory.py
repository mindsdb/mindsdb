import os

from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ExecutorClientFactory:
    def __init__(self):
        self.host = os.environ.get("MINDSDB_EXECUTOR_SERVICE_HOST")
        self.port = os.environ.get("MINDSDB_EXECUTOR_SERVICE_PORT")
        if self.host is not None and self.port is not None:
            try:
                from mindsdb.api.mysql.mysql_proxy.executor.executor_grpc_client import ExecutorClientGRPC
                self.client_class = ExecutorClientGRPC
            except (ImportError, ModuleNotFoundError):
                logger.error("to use microservice mode please install 'pip install mindsdb[grpc]'")
                self.host = None
                self.port = None

    def __call__(self, session, sqlserver):
        if self.host is None or self.port is None:
            logger.info(
                "%s.__call__: no post/port to ExecutorService have provided. Local Executor instance will use",
                self.__class__.__name__,
            )
            return Executor(session, sqlserver)

        logger.info("%s.__call__: api to communicate with db services - gRPC",
                    self.__class__.__name__,
                    )

        return self.client_class(session, sqlserver)


ExecutorClient = ExecutorClientFactory()
