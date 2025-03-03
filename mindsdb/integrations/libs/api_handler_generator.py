from dataclasses import dataclass
import re
import json
from typing import Dict, List, Tuple
import yaml

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
)
from mindsdb.integrations.libs.api_handler import APIResource


@dataclass
class APIInfo:
    """
    A class to store the information about the API.
    """
    base_url: str = None
    auth: dict = None


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
class APIResourceType:
    type_name: str
    sub_type: str = None
    properties: dict[str, str] = None


@dataclass
class APIEndpointParam:
    name: str
    type: APIResourceType
    where: str = None
    optional: bool = False


def find_common_url_prefix(urls):
    urls = [
        url.split('/')
        for url in urls
    ]

    min_len = min(len(s) for s in urls)

    for i in range(min_len):
        for j in range(1, len(urls)):
            if urls[j][i] != urls[0][i]:
                return '/'.join(urls[0][:i])

    return '/'.join(urls[0][:min_len])


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


class APIResourceGenerator:
    """
    A class to generate API resources based on the OpenAPI specification.
    """
    def __init__(self, url, connection_data, api_base=None) -> None:
        self.openapi_spec_parser = OpenAPISpecParser(url)
        self.connection_data = connection_data
        self.api_base = api_base

    def generate_api_resources(self, handler, table_name_format='{url}') -> Dict[str, APIResource]:
        """
        Generates an API resource based on the OpenAPI specification.

        Returns:
            Type[APIResource]: The generated API resource class.
        """
        paths = self.openapi_spec_parser.get_paths()
        schemas = self.openapi_spec_parser.get_schemas()
        self.security_schemes = self.openapi_spec_parser.get_security_schemes()

        self.resource_types = self.process_resource_types(schemas)
        endpoints = self.process_endpoints(paths)

        prefix_len = len(find_common_url_prefix([i.url for i in endpoints]))

        resources = {}
        for endpoint in endpoints:
            url = endpoint.url[prefix_len:]
            url = re.sub(r'[\{\}/]+', '_', url).strip('_')
            table_name = table_name_format.format(url=url, method=endpoint.method)
            resources[table_name] = RestApiTable(handler, endpoint=endpoint, resource_gen=self)

        return resources

    def process_resource_types(self, schemas: dict) -> dict:
        resource_types = {}
        for name, schema_info in schemas.items():
            resource_types[name] = self._convert_to_resource_type(schema_info)

        return resource_types

    def has_pagination(self, params):
        if 'startAt' in params:
            return True
        return False

    def process_endpoints(self, paths: dict) -> List[APIEndpoint]:
        """
        Processes the endpoints defined in the OpenAPI specification.

        Args:
            endpoints (Dict): A dictionary containing the endpoints defined in the OpenAPI specification.

        Returns:
            Dict: A dictionary containing the processed endpoints.
        """
        endpoints = []
        for path, path_info in paths.items():
            if self.api_base is not None and not path.startswith(self.api_base):
                continue

            for http_method, method_info in path_info.items():
                if http_method != 'get':
                    continue

                parameters = self._process_endpoint_parameters(method_info['parameters']) if 'parameters' in method_info else {}

                response, response_path = self._process_endpoint_response(method_info['responses'])
                if response is None:
                    continue

                has_pagination = self.has_pagination(parameters)

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
        endpoint_parameters = {}
        for parameter in parameters:
            type_name = self.get_resource_type(parameter['schema'])

            optional = 'default' in parameter['schema']
            endpoint_parameters[parameter['name']] = APIEndpointParam(
                name=parameter['name'],
                type=type_name,
                optional=optional,
                where=parameter['in'],
            )

        return endpoint_parameters

    def _process_endpoint_response(self, responses: dict) -> Tuple[str, list]:
        response = None
        response_path = []  # used to find list in response

        if '200' not in responses:
            return responses, response_path

        for content_type, resp_info in responses['200']['content'].items():
            if content_type != 'application/json':
                continue

            # type_name=get_type(resp_info['schema'])
            if 'schema' not in resp_info:
                continue

            type = self._convert_to_resource_type(resp_info['schema'])

            # resolve type
            if type.type_name in self.resource_types:
                type = self.resource_types[type.type_name]

            if type.type_name == 'list':
                response = type.sub_type
            elif type.type_name == 'object':
                # find property with list
                if type.properties is None:
                    raise NotImplementedError
                for k, v in type.properties.items():
                    if v.type_name == 'array':
                        response = v.sub_type
                        response_path.append(k)
                        break
            break

        return response, response_path

    def _convert_to_resource_type(self, schema: dict) -> APIResourceType:
        """
        Converts the schema information to a resource type.

        Args:
            schema (Dict): A dictionary containing the schema information.

        Returns:
            APIResourceType: An object containing the resource type information.
        """
        type_name = self.get_resource_type(schema)
        # type_name= info['type']

        kwargs = {
            # 'name': name,
            'type_name': type_name,
        }

        if type_name == 'object':
            properties = {}
            if 'properties' in schema:
                for k, v in schema['properties'].items():
                    # type_name2 = get_type(v)
                    properties[k] = self._convert_to_resource_type(v)
            elif 'additionalProperties' in schema:
                if isinstance(schema['additionalProperties'], dict) and 'type' in schema['additionalProperties']:
                    type_name = schema['additionalProperties']['type']
                else:
                    type_name = 'string'

            kwargs['properties'] = properties
        if type_name == 'array' and 'items' in schema:
            kwargs['sub_type'] = self.get_resource_type(schema['items'])

        return APIResourceType(**kwargs)

    def get_resource_type(self, schema: dict) -> str:
        if 'type' in schema:
            return schema['type']

        elif '$ref' in schema:
            return schema['$ref'].split('/')[-1]

        elif 'allOf' in schema:
            # TODO Get only the first type.
            return self.get_resource_type(schema['allOf'][0])

        else:
            return None


class RestApiTable(APIResource):
    def __init__(self, *args, endpoint: APIEndpoint = None, resource_gen=None, **kwargs):
        self.endpoint = endpoint
        resource_types = resource_gen.resource_types
        self.connection_data = resource_gen.connection_data
        self.security_schemes = resource_gen.security_schemes

        self.output_columns = {}
        if endpoint.response in resource_types:
            self.output_columns = resource_types[endpoint.response].properties
        else:
            # let it be single column with this type
            self.output_columns = {'value': endpoint.response}

        # check params:
        self.params, self.list_params = [], []
        for name, param in endpoint.params.items():
            self.params.append(name)
            if param.type == 'array':
                self.list_params.append(name)

        super().__init__(*args, **kwargs)

    def repr_value(self, value):
        if isinstance(value, dict):
            value = {
                k: v
                for k, v in value.items()
                if v is not None
            }
            value = json.dumps(value)
        elif isinstance(value, list):
            value = ",".join([str(i) for i in value])
        return value

    def _handle_auth(self) -> dict:
        """
        Processes the authentication mechanism defined in the OpenAPI specification.
        Args:
            security_schemes (Dict): A dictionary containing the security schemes defined in the OpenAPI specification.
        Returns:
            Dict: A dictionary containing the authentication information required to connect to the API.
        """
        # API key authentication will be given preference over other mechanisms.
        # NOTE: If the API supports multiple authentication mechanisms, should they be supported? Which one should be given preference?

        security_schemes = self.security_schemes
        if 'ApiKeyAuth' in security_schemes:
            # For API key authentication, the API key is required.
            if 'api_key' not in self.connection_data:
                raise ValueError(
                    "The API key is required for API key authentication."
                )
            return {
                "type": "api_key",
                "credentials": self.connection_data["api_key"],
                "in": security_schemes['ApiKeyAuth']['in'],
                "name": security_schemes['ApiKeyAuth']['name']
            }

        elif 'basicAuth' in security_schemes:
            # For basic authentication, the username and password are required.
            if not all(
                key in self.connection_data
                for key in ["username", "password"]
            ):
                raise ValueError(
                    "The username and password are required for basic authentication."
                )
            return {
                "type": "basic",
                "credentials": HTTPBasicAuth(
                    self.connection_data["username"],
                    self.connection_data["password"],
                ),
            }

        # TODO: Add support for other authentication mechanisms.

    def get_columns(self) -> List[str]:
        return list(self.output_columns.keys())

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ) -> pd.DataFrame:

        if limit is None:
            limit = 20

        query, body, path_vars = {}, {}, {}

        # TODO sort
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
                    name, value = filter
                    param = self.endpoint.params[condition.column]
                    if param.where == 'query':
                        query[name] = value
                    elif param.where == 'path':
                        path_vars[name] = value
                    else:
                        body[name] = value

        url = self.connection_data['api_base'] + self.endpoint.url
        if path_vars:
            url = url.format(**path_vars)
        # check empty placeholders
        placeholders = re.findall(r"{(\w+)}", url)
        if placeholders:
            raise RuntimeError('Parameters are required: ' + ', '.join(placeholders))

        auth = self._handle_auth()['credentials']
        req = requests.request(self.endpoint.method, url, params=query, data=body, auth=auth)

        resp = req.json()
        for item in self.endpoint.response_path:
            resp = resp[item]

        data = []
        count = 0

        columns = self.get_columns()
        for record in resp:
            item = {}

            if isinstance(record, dict):
                for name, value in record.items():

                    # value = record.[name]

                    item[name] = self.repr_value(value)

                data.append(item)
            elif len(columns) > 0:
                # response is value
                item[columns[0]] = self.repr_value(record)

            count += 1
            if limit <= count:
                break

        return pd.DataFrame(data, columns=columns)
