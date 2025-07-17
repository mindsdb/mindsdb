from dataclasses import dataclass
import re
from io import StringIO
import json
from typing import Dict, List, Any
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
)
from mindsdb.integrations.libs.api_handler import APIResource


class ApiRequestException(Exception):
    pass


class ApiResponseException(Exception):
    pass


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
    params: dict
    response: dict


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
    default: Any = None


def find_common_url_prefix(urls):
    if len(urls) == 0:
        return ''
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

            if openapi_spec_path.endswith('.json'):
                self.openapi_spec = response.json()
            else:
                stream = StringIO(response.text)
                self.openapi_spec = yaml.load(stream, Loader=Loader)
        else:
            raise ApiRequestException('URL is required')

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

    def get_specs(self) -> dict:
        return self.openapi_spec


class APIResourceGenerator:
    """
    A class to generate API resources based on the OpenAPI specification.
    """
    def __init__(self, url, connection_data, url_base=None, options=None) -> None:
        self.openapi_spec_parser = OpenAPISpecParser(url)
        self.connection_data = connection_data
        self.url_base = url_base
        self.options = options or {}
        self.resources = {}

    def check_connection(self):
        if 'check_connection_table' in self.options:
            table = self.resources.get(self.options['check_connection_table'])
            if table:
                table.list(targets=[], limit=1, conditions=[])

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

        for endpoint in endpoints:
            url = endpoint.url[prefix_len:]
            # replace placehoders with x
            url = re.sub(r"{(\w+)}", 'x', url)
            url = url.replace('/', '_').strip('_')
            table_name = table_name_format.format(url=url, method=endpoint.method).lower()
            self.resources[table_name] = RestApiTable(handler, endpoint=endpoint, resource_gen=self)

        return self.resources

    def process_resource_types(self, schemas: dict) -> dict:
        resource_types = {}
        for name, schema_info in schemas.items():
            resource_types[name] = self._convert_to_resource_type(schema_info)

        return resource_types

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
            # filter endpoints by url base
            if self.url_base is not None and (not path.startswith(self.url_base) or path == self.url_base):
                continue

            for http_method, method_info in path_info.items():
                if http_method != 'get':
                    continue

                parameters = self._process_endpoint_parameters(method_info['parameters']) if 'parameters' in method_info else {}

                response = self._process_endpoint_response(method_info['responses'])
                if response['type'] is None:
                    continue

                endpoint = APIEndpoint(
                    url=path,
                    method=http_method,
                    params=parameters,
                    response=response
                )

                endpoints.append(endpoint)

        return endpoints

    def get_ref_object(self, ref):
        # get object by $ref link
        el = self.openapi_spec_parser.get_specs()
        for path in ref.lstrip('#').split('/'):
            if path:
                el = el[path]
        return el

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
            if '$ref' in parameter:
                parameter = self.get_ref_object(parameter['$ref'])

            type_name = self.get_resource_type(parameter['schema'])

            endpoint_parameters[parameter['name']] = APIEndpointParam(
                name=parameter['name'],
                type=type_name,
                default=parameter['schema'].get('default'),
                where=parameter['in'],
            )

        return endpoint_parameters

    def _process_endpoint_response(self, responses: dict):
        response = None
        response_path = []  # used to find list in response

        if '200' not in responses:
            return {'type': None}

        view = 'table'

        resp_success = responses['200']
        if '$ref' in resp_success:
            resp_success = self.get_ref_object(responses['200']['$ref'])

        for content_type, resp_info in resp_success['content'].items():
            if content_type != 'application/json':
                continue

            # type_name=get_type(resp_info['schema'])
            if 'schema' not in resp_info:
                continue

            resource_type = self._convert_to_resource_type(resp_info['schema'])

            # resolve type
            type_name = None
            if resource_type.type_name in self.resource_types:
                type_name = resource_type.type_name
                resource_type = self.resource_types[resource_type.type_name]

            if resource_type.type_name == 'array':
                response = resource_type.sub_type
            elif resource_type.type_name == 'object':
                if resource_type.properties is None:
                    raise NotImplementedError

                # if it is a table find property with list
                is_table = False
                if 'total_column' in self.options:
                    for col in self.options['total_column']:
                        if col in resource_type.properties:
                            is_table = True

                if is_table:
                    for k, v in resource_type.properties.items():
                        if v.type_name == 'array':

                            response = v.sub_type
                            response_path.append(k)
                            break
                else:
                    response = type_name
                    view = 'record'
            break

        return {
            'type': response,
            'path': response_path,
            'view': view
        }

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


class RestApiTable(APIResource):
    def __init__(self, *args, endpoint: APIEndpoint = None, resource_gen=None, **kwargs):
        self.endpoint = endpoint
        resource_types = resource_gen.resource_types
        self.connection_data = resource_gen.connection_data
        self.security_schemes = resource_gen.security_schemes
        self.options = resource_gen.options

        self.output_columns = {}
        response_type = endpoint.response['type']
        if response_type in resource_types:
            self.output_columns = resource_types[response_type].properties
        else:
            # let it be single column with this type
            self.output_columns = {'value': response_type}

        # check params:
        self.params, self.list_params = [], []
        for name, param in endpoint.params.items():
            self.params.append(name)
            if param.type == 'array':
                self.list_params.append(name)

        super().__init__(*args, **kwargs)

    def repr_value(self, value):
        # convert dict and lists to strings to show it response table

        if isinstance(value, dict):
            # remove empty keys
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

        if 'token' in self.connection_data:
            headers = {'Authorization': f'Bearer {self.connection_data["token"]}'}

            return {
                "headers": headers
            }

        elif 'basicAuth' in security_schemes:
            # For basic authentication, the username and password are required.
            if not all(
                key in self.connection_data
                for key in ["username", "password"]
            ):
                raise ApiRequestException(
                    "The username and password are required for basic authentication."
                )
            return {
                "auth": HTTPBasicAuth(
                    self.connection_data["username"],
                    self.connection_data["password"],
                ),
            }
        return {}

    def get_columns(self) -> List[str]:
        return list(self.output_columns.keys())

    def get_setting_param(self, setting_name: str) -> str:
        # find input param name for specific setting

        if setting_name in self.options:
            for col in self.options[setting_name]:
                if col in self.endpoint.params:
                    return col

    def get_user_params(self):
        params = {}
        for k, v in self.connection_data.items():
            if k not in ('username', 'password', 'token', 'api_base'):
                params[k] = v
        return params

    def _api_request(self, filters):
        query, body, path_vars = {}, {}, {}
        for name, value in filters.items():
            param = self.endpoint.params[name]
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
            raise ApiRequestException('Parameters are required: ' + ', '.join(placeholders))

        kwargs = self._handle_auth()
        req = requests.request(self.endpoint.method, url, params=query, data=body, **kwargs)

        if req.status_code != 200:
            raise ApiResponseException(req.text)
        resp = req.json()

        total = None
        if 'total_column' in self.options and isinstance(resp, dict):
            for col in self.options['total_column']:
                if col in resp:
                    total = resp[col]
                    break

        for item in self.endpoint.response['path']:
            resp = resp[item]

        if self.endpoint.response['view'] == 'record':
            # response is one record, make table
            resp = [resp]
        return resp, total

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

        filters = {}
        if conditions:
            for condition in conditions:
                if condition.column not in self.params:
                    continue

                if condition.column in self.list_params:
                    if condition.op == FilterOperator.IN:
                        filters[condition.column] = condition.value
                    elif condition.op == FilterOperator.EQUAL:
                        filters[condition.column] = [condition]
                    condition.applied = True
                else:
                    filters[condition.column] = condition.value
                    condition.applied = True

        # user params
        params = self.get_user_params()
        if params:
            filters.update(params)

        page_size_param = self.get_setting_param('page_size_param')
        page_size = None
        if page_size_param is not None:
            # use default value for page size
            page_size = self.endpoint.params[page_size_param].default
            if page_size:
                filters[page_size_param] = page_size
        resp, total = self._api_request(filters)

        # pagination
        offset_param = self.get_setting_param('offset_param')
        page_num_param = self.get_setting_param('page_num_param')
        if offset_param is not None or page_num_param is not None:
            page_num = 1
            while True:
                count = len(resp)
                if limit <= count:
                    break

                if total is not None and total <= count:
                    # total is reached
                    break

                if page_size is not None and page_size > count:
                    # number of results are more than page, don't go to next page
                    break

                # download more pages
                if offset_param:
                    filters[offset_param] = count
                else:
                    page_num += 1
                    filters[page_num_param] = page_num
                resp2, total = self._api_request(filters)
                if len(resp2) == 0:
                    # no results from next page
                    break
                resp.extend(resp2)

        resp = resp[:limit]

        data = []

        columns = self.get_columns()
        for record in resp:
            item = {}

            if isinstance(record, dict):
                for name, value in record.items():
                    item[name] = self.repr_value(value)

                data.append(item)
            elif len(columns) > 0:
                # response is value
                item[columns[0]] = self.repr_value(record)

        return pd.DataFrame(data, columns=columns)
