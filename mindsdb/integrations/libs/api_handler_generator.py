from dataclasses import dataclass
import json
from typing import List, Optional, Type
import yaml

import pandas as pd
from requests.auth import HTTPBasicAuth

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, SortColumn
)
from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)


@dataclass
class APIInfo:
    """
    A class to store the information about the API.
    """
    auth: dict = None
    pagination: dict = None


@dataclass
class APIEndpoint:
    url: str
    method: str
    # table_name: str
    params: dict
    response: str
    response_path: list
    has_pagination: bool = False


@dataclass
class APIEndpointParam:
    name: str
    type: str
    where: str = None
    optional: bool = False


@dataclass
class APISchema:
    name: str
    type_name: str
    sub_type: str = None
    properties: dict[str, str] = None


class APIResourceGenerator:
    """
    A class to generate API resources based on the OpenAPI specification.
    """
    def __init__(self, openapi_spec_path: str):
        self.openapi_spec_parser = OpenAPISpecParser(openapi_spec_path)

    def generate_api_resource(self) -> Type[APIResource]:
        """
        Generates an API resource based on the OpenAPI specification.

        Returns:
            Type[APIResource]: The generated API resource class.
        """
        
        class AnyResource(APIResource):
            def list(
                self,
                conditions: Optional[List[FilterCondition]] = None,
                limit: Optional[int] = None,
                sort: Optional[List[SortColumn]] = None,
                targets: Optional[List[str]] = None,
                **kwargs,   
            ) -> pd.DataFrame:
                pass

        return AnyResource


class APIHandlerGenerator:
    """
    A class to generate an API handler based on the OpenAPI specification.
    """
    def __init__(self, openapi_spec_path: str):
        self.openapi_spec_parser = OpenAPISpecParser(openapi_spec_path)

    def generate_api_handler(self, resources: dict[str, Type[APIResource]]) -> Type[APIHandler]:
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

                self.api_info = {}
                self.is_connected = False

                for resource, resource_class in resources.items():
                    self._register_table(resource, resource_class(self))

            def connect(self) -> dict:
                """
                Establishes a connection to the API.
                
                Returns:
                    dict: A dictionary containing the information required to connect to the API.
                          Most notably, the authentication mechansim and the associated credentials.
                          Additionally, if pagination is supported, the pagination information will be included.
                          API resources will need to parse this information to make requests to the API.
                """
                if self.is_connected is True:
                    return self.api_info
                
                # If the API requires authentication, set up the authentication mechanism.
                auth = None
                if security_schemes:
                    auth = APIHandlerGenerator.process_auth(
                        self.connection_data, security_schemes
                    )

                # TODO: Add support for pagination if the API supports it.
                pagination = None

                self.api_info = APIInfo(auth=auth, pagination=pagination)
                        
            def check_connection(self) -> StatusResponse:
                """
                Checks the connection to the API.

                Returns:
                    StatusResponse: An object containing the success status and an error message if an error occurs.
                """
                # TODO: Implement the connection check logic by making a simple request to the API.
                #       Can the endpoint be determined by looking for common 'test' or 'health' endpoints such as '/me' or '/health'?
                pass\
                
        return AnyHandler

    @staticmethod
    def process_auth(connection_data: dict, security_schemes: dict) -> dict:
        """
        Processes the authentication mechanism defined in the OpenAPI specification.

        Args:
            connection_data (Dict): A dictionary containing the connection data required to connect to the API.
            security_schemes (Dict): A dictionary containing the security schemes defined in the OpenAPI specification.

        Returns:
            Dict: A dictionary containing the authentication information required to connect to the API.
        """
        # API key authentication will be given preference over other mechanisms.
        # NOTE: If the API supports multiple authentication mechanisms, should they be supported? Which one should be given preference?
        if 'ApiKeyAuth' in security_schemes:
            # For API key authentication, the API key is required.
            if 'api_key' not in connection_data:
                raise ValueError(
                    "The API key is required for API key authentication."
                )
            return {
                "type": "api_key",
                "credentials": connection_data["api_key"],
                "in": security_schemes['ApiKeyAuth']['in'],
                "name": security_schemes['ApiKeyAuth']['name']
            }

        elif 'basic' in security_schemes:
            # For basic authentication, the username and password are required.
            if not all(
                key in connection_data
                for key in ["username", "password"]
            ):
                raise ValueError(
                    "The username and password are required for basic authentication."
                )
            return {
                "type": "basic",
                "credentials": HTTPBasicAuth(
                    connection_data["username"],
                    connection_data["password"],
                ),
            }

        # TODO: Add support for other authentication mechanisms.

    @staticmethod
    def process_pagination(connection_data: dict, pagination: dict) -> dict:
        """
        Processes the pagination information defined in the OpenAPI specification.

        Args:
            pagination (Dict): A dictionary containing the pagination information defined in the OpenAPI specification.

        Returns:
            Dict: A dictionary containing the pagination information required to paginate through the API responses.
        """
        pass


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

    def get_schemas(self) -> dict:
        """
        Returns the schemas defined in the OpenAPI specification.
        
        Returns:
            dict: A dictionary containing the schemas defined in the OpenAPI specification.
        """
        return self.openapi_spec.get('components', {}).get('schemas', {})

    def get_endpoints(self) -> dict:
        """
        Returns the endpoints defined in the OpenAPI specification.
        
        Returns:
            dict: A dictionary containing the endpoints defined in the OpenAPI specification.
        """
        return self.openapi_spec.get('paths', {})