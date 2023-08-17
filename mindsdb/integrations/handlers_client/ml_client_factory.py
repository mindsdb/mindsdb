import os
from mindsdb.integrations.libs.handler_helpers import discover_services
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class MLClientFactory:
    def __init__(self, handler_class, engine):
        self.engine = engine
        self.client_class = None
        # self.client_class = MLClientGRPC
        self.handler_class = handler_class
        self.__name__ = self.handler_class.__name__
        self.__module__ = self.handler_class.__module__

    def __call__(self, engine_storage, model_storage, **kwargs):
        service_info = self.discover_service(self.engine)
        if service_info:
            host = service_info["host"]
            port = service_info["port"]
        else:
            host = os.environ.get("MINDSDB_ML_SERVICE_HOST", None)
            port = os.environ.get("MINDSDB_ML_SERVICE_PORT", None)

        if host is not None and port is not None:
            try:
                from mindsdb.integrations.handlers_client.ml_grpc_client import MLClientGRPC
                self.client_class = MLClientGRPC
            except (ImportError, ModuleNotFoundError):
                logger.error("to use microservice mode please install 'pip install mindsdb[grpc]'")
                host = None
                port = None

        if host is None or port is None:
            logger.info(
                "%s.__call__: no post/port to MLService have provided. Handle all ML request locally",
                self.__class__.__name__,
            )
            return self.handler_class(engine_storage=engine_storage, model_storage=model_storage, **kwargs)

        logger.info("%s.__call__: api to communicate with ML services - gRPC, host - %s, port - %s",
                    self.__class__.__name__,
                    host,
                    port,
                    )
        kwargs["engine"] = self.engine
        return self.client_class(host, port, integration_id=engine_storage.integration_id, predictor_id=model_storage.predictor_id, **kwargs)

    def discover_service(self, engine):
        discover_url = os.environ.get("REGISTRY_URL")
        if not discover_url:
            return {}
        discover_url = f"{discover_url}/discover"
        res = discover_services(discover_url)
        if not res:
            return {}
        return res[engine][0]
