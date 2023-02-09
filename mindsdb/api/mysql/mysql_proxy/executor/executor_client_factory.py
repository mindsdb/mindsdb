import os

from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_client import ExecutorClient as ExecutorClientREST
from mindsdb.api.mysql.mysql_proxy.executor.executor_grpc_client import ExecutorClientGRPC

from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")

class ExecutorClientFactory:
    def __init__(self):
        self.api_type = os.environ.get("MINDSDB_INTERCONNECTION_API", "rest").lower()
        if self.api_type == 'grpc':
            self.client_class = ExecutorClientGRPC
        else:
            self.client_class = ExecutorClientREST

        self.host = os.environ.get("MINDSDB_EXECUTOR_SERVICE_HOST")
        self.port = os.environ.get("MINDSDB_EXECUTOR_SERVICE_PORT")

    def __call__(self, session, sqlserver):
        if self.host is None or self.port is None:
            logger.info(
                "%s.__call__: no post/port to ExecutorService have provided. Local Executor instance will use",
                self.__class__.__name__,
            )
            return Executor(session, sqlserver)
        
        logger.info("%s.__call__: api to communicate with db services - %s",
                    self.__class__.__name__,
                    self.api_type,
                    )

        return self.client_class(session, sqlserver)


ExecutorClient = ExecutorClientFactory()
