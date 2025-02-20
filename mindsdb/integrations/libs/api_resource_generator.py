from typing import List, Dict, Optional
import json

import pandas as pd
import requests

from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
)
from mindsdb.integrations.libs.api_handler import APIResource


class APIResourceGenerator:
    def __init__(self, base_url: str, openapi_file: str = "openapi.json"):
        self.base_url = base_url
        self.openapi_spec = json.load(open(openapi_file, 'r'))

    def _get_resources(self) -> Dict:
        """
        Retrieves the resources from the OpenAPI spec.
        """
        return self.openapi_spec.get('paths', {})
    
    def _get_schemas(self) -> Dict:
        """
        Retrieves the schemas from the OpenAPI spec.
        """
        return self.openapi_spec.get('components', {}).get('schemas', {})
    
    def _group_resources(self) -> Dict:
        """
        Groups the resources in the OpenAPI spec by their common path.
        """
        resources = self._get_resources()
        resource_groups = {}
        for key, value in resources.items():
            resource = key.split('/')[1]
            if resource not in resource_groups:
                resource_groups[resource] = [{key: value}]
            else:
                resource_groups[resource].append({key: value})
        return resource_groups
    
    def _create_resource_class(self, resource: str, resource_group: List[Dict]) -> APIResource:
        """
        Creates a new APIResource class based on the OpenAPI spec.
        """
        base_url = self.base_url
        class AnyTable(APIResource):
            "This is a table abstraction for any resource of the OpenAPI spec."

            def list(
                self,
                conditions: Optional[List[FilterCondition]] = None,
                limit: Optional[int] = None,
                sort: Optional[List[SortColumn]] = None,
                targets: Optional[List[str]] = None,
                **kwargs,   
            ) -> pd.DataFrame:
                """
                Retrieves a list of resources from the OpenAPI spec based on the given conditions.
                This method assumes that resources are retrieved using GET requests.
                This method will only handle conditions (if possible).
                Sorting, limiting, and selecting columns will be handled by the parent.
                """
                endpoint = f"/{resource}"

                if conditions:
                    supported_endpoints = [endpoint for group in resource_group for endpoint in group]
                    for condition in conditions:
                        if condition.op == FilterOperator.EQUAL:
                            path = f"/{resource}/{{{condition.column}}}"
                            if path in supported_endpoints:
                                endpoint = f"/{resource}/{condition.value}"
                                break
                        elif condition.op == FilterOperator.IN:
                            pass

                        condition.applied = True

                results = self._get(endpoint, **kwargs)
                # TODO: How can we handle nested data here?
                return pd.DataFrame(results if isinstance(results, list) else [results])

            def _get(self, endpoint: str, **kwargs) -> List[Dict]:
                """
                Makes a GET request and returns the JSON response.
                """
                # TODO: How can pagination be handled here?
                json_response = requests.get(base_url + endpoint, **kwargs).json()
                return json_response
            
            def get_columns(self) -> List[str]:
                # Get the singular form of the resource name.
                # TODO: This is a naive implementation. Improve this.
                singular_resource = resource[:-1]

                schema = self._get_schemas().get(singular_resource, {})
                return list(schema.get('properties', {}).keys())

        return AnyTable

    def create_resource_classes(self) -> List[tuple]:
        """
        Creates a class for each resource in the OpenAPI spec.
        """
        resource_groups = self._group_resources()
        
        resource_classes = []
        for resource, resource_group in resource_groups.items():
            resource_class = self._create_resource_class(resource, resource_group)
            resource_classes.append((resource, resource_class))

        return resource_classes