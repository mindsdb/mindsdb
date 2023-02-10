import os
from mindsdb.integrations.handlers_client.db_grpc_client import DBClientGRPC
from mindsdb.integrations.libs.handler_helpers import get_handler, discover_services
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class DBClientFactory:
    def __init__(self):
        self.client_class = DBClientGRPC



    def __call__(self, handler_type: str, **kwargs: dict):
        service_info = self.discover_service() 
        if service_info:
           host = service_info["host"]
           port = service_info["port"]
        else:
            host = os.environ.get("MINDSDB_ML_SERVICE_HOST", None)
            port = os.environ.get("MINDSDB_ML_SERVICE_PORT", None)

        if host is None or port is None:
            logger.info(
                "%s.__call__: no post/port to DBService have provided. Handle all db request locally",
                self.__class__.__name__,
            )
            handler_class = get_handler(handler_type)
            return handler_class(**kwargs)
        
        logger.info("%s.__call__: api to communicate with db services - gRPC",
                    self.__class__.__name__,
                    )

        return self.client_class(handler_type, host=host, port=port, handler_params=kwargs)

    def discover_service(self):
        discover_url = os.environ.get("REGISTRY_URL")
        if not discover_url:
            return {}
        discover_url = f"{discover_url}/discover"
        res = discover_services(discover_url)
        if not res:
            return {}
        _type = self.handler_params.get("integration_engine") or self.handler_params.get("name", None)
        return res[_type][0]


DBClient = DBClientFactory()
