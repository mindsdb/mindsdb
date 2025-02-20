import json

from pandas import pd
import requests

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
)
from mindsdb.integrations.libs.api_handler import APIResource


class APIResourceGenerator:
    def __init__(self, base_url: str, openapi_file: str = "openapi.json"):
        self.base_url = base_url
        self.openapi_spec = json.load(open(openapi_file, 'r'))

    def _get_resources(self):
        """
        Retrieves the resources from the OpenAPI spec.
        """
        return self.openapi_spec.get('paths', {}).keys()
    
    def _get_schemas(self):
        """
        Retrieves the schemas from the OpenAPI spec.
        """
        return self.openapi_spec.get('components', {}).get('schemas', {})
    
    def _group_resources(self):
        """
        Groups the resources in the OpenAPI spec by their common path.
        """
        resources = self._get_resources()
        grouped_resources = {}
        for resource in resources:
            path = resource.split('/')[1]
            if path not in grouped_resources:
                grouped_resources[path] = [resource]
            else:
                grouped_resources[path].append(resource)
        return grouped_resources
    
    def _create_resource_class(self, resource: str, resource_group: list[dict]) -> APIResource:
        """
        Creates a new APIResource class based on the OpenAPI spec.
        """
        class AnyTable(APIResource):
            "This is a table abstraction for any resource of the OpenAPI spec."

            def list(
                self,
                conditions: list[FilterCondition] = None,
                limit: int = None,
                sort: list[SortColumn] = None,
                targets: list[str] = None,
                **kwargs,   
            ) -> pd.DataFrame:
                """
                Retrieves a list of resources from the OpenAPI spec based on the given conditions.
                This method assumes that resources are retrieved using GET requests.
                This method will only handle conditions (if possible).
                Sorting, limiting, and selecting columns will be handled by the parent.
                """
                if not conditions:
                    endpoint = resource

                else:
                    for condition in conditions:
                        if condition.op == FilterOperator.EQUAL:
                            path = f"/{resource}/{{{condition.column}}}"
                            if path in resource_group:
                                endpoint = f"/{resource}/{condition.value}"
                                break
                        elif condition.op == FilterOperator.IN:
                            pass

                results = self._get(endpoint, **kwargs)
                # TODO: How can we handle nested data here?
                return pd.DataFrame(results)

            def _get(self, endpoint: str, **kwargs) -> list[dict]:
                """
                Makes a GET request and returns the JSON response.
                """
                # TODO: How can pagination be handled here?
                json_response = requests.get(self.base_url + endpoint, **kwargs).json()
                return json_response
            
            def get_columns(self):
                # Get the singular form of the resource name.
                # TODO: This is a naive implementation. Improve this.
                singular_resource = resource[:-1]

                schema = self._get_schemas().get(singular_resource, {})
                return schema.get('properties', {}).keys()

        return AnyTable

    def create_resource_classes(self):
        """
        Creates a class for each resource in the OpenAPI spec.
        """
        grouped_resources = self._group_resources()
        
        resource_classes = []
        for resource, resource_group in grouped_resources.items():
            resource_class = self._create_resource_class(resource, resource_group)
            resource_classes.append(resource_class)

        return resource_classes