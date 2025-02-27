import json
import yaml
from typing import Optional, Type

from requests.auth import HTTPBasicAuth

from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)


class APIHandlerGenerator:
    """
    A class to generate an API handler based on the OpenAPI specification.
    """
    def __init__(self, openapi_spec_path: str):
        self.openapi_spec_parser = OpenAPISpecParser(openapi_spec_path)

    def generate_api_handler(self, resources: dict[str, Type[APIResource]]):
        """
        Generates an API handler class based on the OpenAPI specification.
        Args:
            resources (Dict[str, Type[APIResource]]): A dictionary containing the resources and their corresponding classes.
        Returns:
            Type[APIHandler]: The generated API handler class.
        """
        security_schemes = self.openapi_spec_parser.get_security_schemes()
        
        class AnyHandler(APIHandler):
            def __init__(self, name: str, connection_data: Optional[dict], **kwargs) -> None:
                """
                Initializes the handler and registers the resources.
                Args:
                    name (Text): The name of the handler instance.
                    connection_data (Dict): The connection data required to connect to the API.
                    kwargs: Arbitrary keyword arguments.
                """
                super().__init__(name)
                self.connection_data = connection_data
                self.kwargs = kwargs

                self.connection = None
                self.is_connected = False

                for resource, resource_class in resources.items():
                    self._register_table(resource, resource_class(self))

            def connect(self) -> dict:
                """
                Establishes a connection to the API.
                
                Returns:
                    dict: A dictionary containing the information required to connect to the API.
                          Most notably, the authentication mechansim and the associated credentials.
                          API resources will need to parse this information to make requests to the API.
                """
                if self.is_connected is True:
                    return self.connection
                
                # If the API requires authentication, set up the authentication mechanism.
                if security_schemes:
                    # API key authentication will be given preference over other mechanisms.
                    # NOTE: If the API supports multiple authentication mechanisms, should they be supported? Which one should be given preference?
                    if 'ApiKeyAuth' in security_schemes:
                        # For API key authentication, the API key is required.
                        if 'api_key' not in self.connection_data:
                            raise ValueError(
                                "The API key is required for API key authentication."
                            )
                        self.connection = {
                            "auth": {
                                "type": "api_key",
                                "credentials": self.connection_data["api_key"],
                                "in": security_schemes['ApiKeyAuth']['in'],
                                "name": security_schemes['ApiKeyAuth']['name']
                            }
                        }

                    elif 'basic' in security_schemes:
                        # For basic authentication, the username and password are required.
                        if not all(
                            key in self.connection_data
                            for key in ["username", "password"]
                        ):
                            raise ValueError(
                                "The username and password are required for basic authentication."
                            )
                        self.connection = {
                            "auth": {
                                "type": "basic",
                                "credentials": HTTPBasicAuth(
                                    self.connection_data["username"],
                                    self.connection_data["password"],
                                ),
                            }
                        }

                    # TODO: Add support for other authentication mechanisms.
                        
            def check_connection(self) -> StatusResponse:
                """
                Checks the connection to the API.

                Returns:
                    StatusResponse: An object containing the success status and an error message if an error occurs.
                """
                # TODO: Implement the connection check logic by making a simple request to the API.
                #       Can the endpoint be determined by looking for common 'test' or 'health' endpoints such as '/me' or '/health'?
                pass

        return AnyHandler


class OpenAPISpecParser:
    """
    A class to parse the OpenAPI specification.
    """
    def __init__(self, openapi_spec_path: str) -> None:
        with open(openapi_spec_path, 'r') as f:
            self.openapi_spec = json.loads(f.read()) if openapi_spec_path.endswith('.json') else yaml.safe_load(f)

    def get_security_schemes(self) -> dict:
        """
        Returns the security schemes defined in the OpenAPI specification.
        
        Returns:
            dict: A dictionary containing the security schemes defined in the OpenAPI specification.
        """
        return self.openapi_spec.get('components', {}).get('securitySchemes', {})