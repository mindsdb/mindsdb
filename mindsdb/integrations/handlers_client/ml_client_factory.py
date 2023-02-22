import os
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.handlers_client.ml_grpc_client import MLClientGRPC
from mindsdb.integrations.libs.handler_helpers import discover_services
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class MLClientFactory:
    def __init__(self):
        self.client_class = MLClientGRPC

    def __call__(self, **kwargs: dict):
        service_info = self.discover_service(kwargs)
        if service_info:
            host = service_info["host"]
            port = service_info["port"]
        else:
            host = os.environ.get("MINDSDB_ML_SERVICE_HOST", None)
            port = os.environ.get("MINDSDB_ML_SERVICE_PORT", None)

        if host is None or port is None:
            logger.info(
                "%s.__call__: no post/port to MLService have provided. Handle all ML request locally",
                self.__class__.__name__,
            )
            return BaseMLEngineExec(**kwargs)

        logger.info("%s.__call__: api to communicate with ML services - gRPC, host - %s, port - %s",
                    self.__class__.__name__,
                    host,
                    port,
                    )

        return self.client_class(host, port, kwargs)

    def discover_service(self, handler_params):
        discover_url = os.environ.get("REGISTRY_URL")
        if not discover_url:
            return {}
        discover_url = f"{discover_url}/discover"
        res = discover_services(discover_url)
        if not res:
            return {}
        _type = handler_params.get("integration_engine") or handler_params.get("name", None)
        return res[_type][0]


MLClient = MLClientFactory()
