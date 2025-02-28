from dataclasses import dataclass
import json
from typing import Dict, List, Optional, Tuple, Type
import yaml

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
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
class APISchemaType:
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

    def generate_api_resources(self) -> Dict[str, Type[APIResource]]:
        """
        Generates an API resource based on the OpenAPI specification.

        Returns:
            Type[APIResource]: The generated API resource class.
        """
        paths = self.openapi_spec_parser.get_paths()
        schemas = self.openapi_spec_parser.get_schemas()

        schema_types = self.process_schema_types(schemas)
        endpoints = self.process_endpoints(paths, schema_types)
        
        resources = {}
        for endpoint in endpoints:
            # TODO: Getting the name like this is not reliable.
            name = endpoint.url.split('/')[-1]
            resources[name] = self._generate_api_resource(name, endpoint)

        return resources
    
    def _generate_api_resource(self, name: str, endpoint: APIEndpoint) -> Type[APIResource]:
        """
        Generates an API resource class based on the endpoint information.

        Args:
            name (Text): The name of the API resource.
            endpoint (APIEndpoint): An object containing the endpoint information.

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
                if limit is None:
                    limit = 20

                query = {}
                body = {}
                # if sort is not None and self._allow_sort:
                #     for col in sort:
                #         method_kwargs['sort'] = col.column
                #         method_kwargs['direction'] = 'asc' if col.ascending else 'desc'
                #         sort.applied = True
                #         # supported only 1 column
                #         break

                if conditions:
                    for condition in conditions:
                        filter = None
                        if condition.column not in self.params:
                            continue

                        if condition.column in self.list_params:
                            if condition.op == FilterOperator.IN:
                                filter = [condition.column, condition.value]
                            elif condition.op == FilterOperator.EQUAL:
                                filter = [condition.column, [condition]]
                            condition.applied = True
                        else:
                            filter = [condition.column, condition.value]
                            condition.applied = True

                        if filter:
                            param = endpoint.params[condition.column]
                            if param.where == 'query':
                                query[filter[0]] = [filter[1]]
                            else:
                                body[filter[0]] = [filter[1]]

                response = requests.request(endpoint.method, endpoint.url, params=query, data=body)

                data = []
                count = 0
                # TODO: Check the content type of the response.
                for record in response.json():
                    item = {}
                    for name, type in self.output_columns.items():

                        # workaround to prevent making addition request per property.
                        if name in targets:
                            # request only if is required
                            value = getattr(record, name)
                        else:
                            value = getattr(record, '_' + name).value
                        if value is not None:
                            if type.name == 'list':
                                value = ",".join([
                                        str(self.repr_value(i, type.sub_type))
                                        for i in value
                                ])
                            else:
                                value = self.repr_value(value, type.name)
                        item[name] = value

                    data.append(item)
                    count += 1

                    if limit <= count:
                        break

                return pd.DataFrame(data, columns=self.get_columns())

        return AnyResource
    
    def process_schema_types(self, schemas: dict) -> dict:
        schema_types = {}
        for name, schema_info in schemas.items():
            schema_types[name] = self._convert_to_schema_type(name, schema_info)

        return schema_types

    def process_endpoints(self, paths: dict, schema_types: dict) -> dict:
        """
        Processes the endpoints defined in the OpenAPI specification.

        Args:
            endpoints (Dict): A dictionary containing the endpoints defined in the OpenAPI specification.

        Returns:
            Dict: A dictionary containing the processed endpoints.
        """
        endpoints = []
        for path, path_info in paths.items():
            # TODO: Change this to base URL.
            if path != '/rest/api/3/search':
                continue

            for http_method, method_info in path_info.items():
                if http_method != 'get':
                    continue

                parameters = self._process_endpoint_parameters(method_info['parameters']) if 'parameters' in method_info else {}
                response, response_path = self._process_endpoint_response(method_info['responses'], schema_types)
                # TODO: Add support for pagination.
                has_pagination = False

                endpoint = APIEndpoint(
                    url=path,
                    method=http_method,
                    params=parameters,
                    has_pagination=has_pagination,
                    response=response,
                    response_path=response_path
                )

            endpoints.append(endpoint)

        return endpoints

    def _process_endpoint_parameters(self, parameters: list) -> Dict[str, APIEndpointParam]:
        """
        Processes the parameters defined in the OpenAPI specification.

        Args:
            parameters (Dict): A dictionary containing the parameters defined in the OpenAPI specification.

        Returns:
            Dict: A dictionary containing the processed parameters.
        """
        for parameter in parameters:
            type_name = self.get_schema_type(parameter['schema'])

            optional = 'default' in parameter['schema']
            parameters[parameter['name']] = APIEndpointParam(
                name=parameter['name'],
                type=type_name,
                optional=optional,
                where=parameter['in'],
            )

        return parameters
    
    def _process_endpoint_response(self, responses: dict, schema_types: dict) -> Tuple[str, str]:
        response = None
        response_path = [] # used to find list in response

        for status_code, response in responses.items():
            if status_code != '200':
                continue

            for content_type, resp_info in response['content'].items():
                if content_type != 'application/json':
                    raise NotImplementedError
                
                type_name = self.get_schema_type(resp_info['schema'])
                # try to find table
                type = schema_types[type_name]
                if type.type_name == 'list':
                    response = type.sub_type
                elif type.type_name == 'object':
                    # find property with list
                    for k, v in type.properties.items():
                        if v.type_name == 'array':
                            response = v.sub_type
                            response_path.append(k)
                            break

            break

        return response, response_path

    def _convert_to_schema_type(self, name: str, schema: dict) -> APISchemaType:
        """
        Converts the schema information to a schema type.

        Args:
            schema (Dict): A dictionary containing the schema information.

        Returns:
            APISchemaType: An object containing the schema type information.
        """
        type_name = self.get_schema_type(schema)
        # type_name= info['type']

        kwargs = {
            'name': name,
            'type_name': type_name,
        }

        if type_name == 'object' and 'properties' in schema:
            properties = {}
            for k, v in schema['properties'].items():
                # type_name2 = get_type(v)
                properties[k] = self._convert_to_schema_type(k, v)

            kwargs['properties'] = properties

        if type_name == 'array' and 'items' in schema:
            kwargs['sub_type'] = self._convert_to_schema_type(schema['items'])

        return APISchemaType(**kwargs)

    def get_schema_type(self, schema: dict) -> str:
        if 'type' in schema:
            return schema['type']

        elif '$ref' in schema:
            return schema['$ref'].split('/')[-1]

        elif 'allOf' in schema:
            # TODO Get only the first type.
            return self.get_schema_type(schema['allOf'][0])

        else:
            return None


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
        if openapi_spec_path.startswith('http://') or openapi_spec_path.startswith('https://'):
            response = requests.get(openapi_spec_path)
            response.raise_for_status()
            self.openapi_spec = response.json() if openapi_spec_path.endswith('.json') else yaml.safe_load(response.text)
        else:
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

    def get_paths(self) -> dict:
        """
        Returns the paths defined in the OpenAPI specification.
        
        Returns:
            dict: A dictionary containing the paths defined in the OpenAPI specification.
        """
        return self.openapi_spec.get('paths', {})