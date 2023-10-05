import os
from mindsdb.integrations.libs.handler_helpers import discover_services
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class MLClientFactory:
    def __init__(self, handler_class, engine):
        self.engine = engine
        self.client_class = None
        self.handler_class = handler_class
        self.__name__ = self.handler_class.__name__
        self.__module__ = self.handler_class.__module__

    def __call__(self, engine_storage, model_storage, **kwargs):
        self.handler_class(engine_storage=engine_storage, model_storage=model_storage, **kwargs)

    def discover_service(self, engine):
        discover_url = os.environ.get("REGISTRY_URL")
        if not discover_url:
            return {}
        discover_url = f"{discover_url}/discover"
        res = discover_services(discover_url)
        if not res:
            return {}
        return res[engine][0]
