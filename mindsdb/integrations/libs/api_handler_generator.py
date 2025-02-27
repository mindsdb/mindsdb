import json
import yaml
from typing import Optional, Type

from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)


class APIHandlerGenerator:
    def __init__(self, openapi_spec_path: str):
        self.openapi_spec_parser = OpenAPISpecParser(openapi_spec_path)

    def generate_api_handler(self, resources: dict[str, Type[APIResource]]):
        
        class AnyHandler(APIHandler):
            def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
                """
                Initializes the handler.
                Args:
                    name (Text): The name of the handler instance.
                    connection_data (Dict): The connection data required to connect to the API.
                    kwargs: Arbitrary keyword arguments.
                """
                super().__init__(name)
                self.connection_data = connection_data
                self.kwargs = kwargs

                for resource, resource_class in resources.items():
                    self._register_table(resource, resource_class(self))

            def connect(self) -> dict:
                """
                Establishes a connection to the API.
                """
                pass
                        
            def check_connection(self) -> StatusResponse:
                """
                Checks the connection to the API.
                """
                pass


class OpenAPISpecParser:
    def __init__(self, openapi_spec_path: str):
        with open(openapi_spec_path, 'r') as f:
            self.openapi_spec = json.loads(f.read()) if openapi_spec_path.endswith('.json') else yaml.safe_load(f)

    def get_security_schemes(self):
        return self.openapi_spec.get('components', {}).get('securitySchemes', {})