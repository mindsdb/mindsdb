import pandas as pd
from ckanapi import RemoteCKAN
from urllib.parse import urlparse, parse_qs

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import (
    HandlerResponse,
    HandlerStatusResponse,
    RESPONSE_TYPE,
)
from mindsdb_sql.parser import ast
from mindsdb.utilities import log
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

logger = log.getLogger(__name__)


class DatasetsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where) if query.where else []
        limit = query.limit.value if query.limit else 1000
        packages = self.list(conditions, limit)
        return pd.DataFrame(packages)

    def list(self, conditions=None, limit=1000):
        self.handler.connect()
        package_list = self.handler.call_ckan_api("package_search", {"rows": limit})
        packages = package_list.get("results", [])

        data = []
        # Get only datastore active resources
        for pkg in packages:
            datastore_active_resources = [
                r for r in pkg.get("resources", []) if r.get("datastore_active")
            ]
            data.append(
                {
                    "id": pkg.get("id"),
                    "name": pkg.get("name"),
                    "title": pkg.get("title"),
                    "num_resources": len(pkg.get("resources", [])),
                    "num_datastore_active_resources": len(datastore_active_resources),
                }
            )

        return pd.DataFrame(data)

    # Define the columns that will be returned by the table
    # Maybe we can make this dynamic in the future
    def get_columns(self):
        return [
            "id",
            "name",
            "title",
            "num_resources",
            "num_datastore_active_resources",
        ]


class ResourceIDsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where) if query.where else []
        limit = query.limit.value if query.limit else 1000

        resources = self.list(conditions, limit)
        return pd.DataFrame(resources)

    def list(self, conditions=None, limit=1000):
        self.handler.connect()
        package_list = self.handler.call_ckan_api("package_search", {"rows": limit})
        packages = package_list.get("results", [])

        data = []
        for package in packages:
            for resource in package.get("resources", []):
                # Get only datastore active resources
                if resource.get("datastore_active"):
                    data.append(
                        {
                            "id": resource.get("id"),
                            "package_id": package.get("id"),
                            "name": resource.get("name"),
                            "format": resource.get("format"),
                            "url": resource.get("url"),
                            "datastore_active": resource.get("datastore_active"),
                        }
                    )
                if len(data) >= limit:
                    break
            if len(data) >= limit:
                break

        return pd.DataFrame(data)

    def get_columns(self):
        return [
            "id",
            "package_id",
            "name",
            "format",
            "url",
            "datastore_active",
        ]


class DatastoreTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where) if query.where else []
        limit = query.limit.value if query.limit else None

        resource_id = None
        other_conditions = []
        for condition in conditions:
            if isinstance(condition, list) and len(condition) == 3:
                op, col, val = condition
                if col == "resource_id" and op == "=":
                    resource_id = val
                else:
                    other_conditions.append(condition)

        if not resource_id:
            message = "Please provide a resource_id in your query. Example: SELECT * FROM datastore WHERE resource_id = 'your_resource_id'"
            df = pd.DataFrame({"message": [message]})
            return df

        data = self.query_datastore(resource_id, other_conditions, limit)
        return pd.DataFrame(data)

    def query_datastore(self, resource_id, conditions, limit):
        params = {
            "resource_id": resource_id,
            "limit": 1000,  # Start with default limit of 1000 records
        }

        # Add filters based on conditions
        filters = {}
        for condition in conditions:
            op, col, val = condition
            if op == "=":
                filters[col] = val
        if filters:
            params["filters"] = filters

        all_records = []
        while True:
            result = self.handler.call_ckan_api("datastore_search", params)
            records = result.get("records", [])
            all_records.extend(records)

            # Check if we've reached the desired limit
            if limit and len(all_records) >= limit:
                logger.info(f"Reached limit of {limit} records")
                all_records = all_records[:limit]
                break

            # Check for next page
            if "_links" in result and "next" in result["_links"]:
                next_url = result["_links"]["next"]
                parsed_url = urlparse(next_url)
                query_params = parse_qs(parsed_url.query)
                params["offset"] = query_params.get("offset", ["0"])[0]
                logger.info(f"Fetching next page with offset {params['offset']}")
            else:
                logger.info("No more pages to fetch")
                break  # No more pages

        df = pd.DataFrame(all_records)

        # Include metadata about the resource
        metadata = self.handler.call_ckan_api("resource_show", {"id": resource_id})
        df["_resource_id"] = resource_id
        df["_resource_name"] = metadata.get("name")
        df["_resource_format"] = metadata.get("format")
        df["_resource_last_modified"] = metadata.get("last_modified")

        return df

    def get_columns(self):
        return [field["id"] for field in self.fields]


class CkanHandler(APIHandler):
    name = "ckan"

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.connection_args = kwargs.get("connection_data", {})

        self.datasets_table = DatasetsTable(self)
        self.resources_table = ResourceIDsTable(self)
        self.datastore_table = DatastoreTable(self)

        self._register_table("datasets", self.datasets_table)
        self._register_table("resources", self.resources_table)
        self._register_table("datastore", self.datastore_table)

    def connect(self):
        if self.is_connected:
            return self.connection

        url = self.connection_args.get("url")
        api_key = self.connection_args.get("api_key")
        if not url:
            raise ValueError("CKAN URL is required")

        try:
            self.connection = RemoteCKAN(url, apikey=api_key)
            self.is_connected = True
            logger.info(f"Successfully connected to CKAN at {url}")
        except Exception as e:
            logger.error(f"Error connecting to CKAN: {e}")
            raise ConnectionError(f"Failed to connect to CKAN: {e}")

        return self.connection

    def check_connection(self) -> HandlerStatusResponse:
        try:
            self.connect()
            return HandlerStatusResponse(success=True)
        except Exception as e:
            logger.error(f"Error checking connection: {e}")
            return HandlerStatusResponse(success=False, error_message=str(e))

    def call_ckan_api(self, method_name: str, params: dict):
        connection = self.connect()
        method = getattr(connection.action, method_name)

        try:
            result = method(**params)
            return result
        except Exception as e:
            logger.error(f"Error calling CKAN API: {e}")
            raise RuntimeError(f"Failed to call CKAN API: {e}")

    def native_query(self, query: str) -> HandlerResponse:
        method, params = self.parse_native_query(query)
        try:
            result = self.call_ckan_api(method, params)
            if isinstance(result, list):
                df = pd.DataFrame(result)
            elif isinstance(result, dict):
                df = pd.DataFrame([result])
            else:
                df = pd.DataFrame([{"result": result}])
            return HandlerResponse(RESPONSE_TYPE.TABLE, df)
        except Exception as e:
            logger.error(f"Error executing native query: {e}")
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=str(e))

    @staticmethod
    def parse_native_query(query: str):
        parts = query.split(":")
        if len(parts) != 2:
            raise ValueError(
                "Invalid query format. Expected 'method_name:param1=value1,param2=value2'"
            )
        method = parts[0].strip()
        params = {}
        if parts[1].strip():
            param_pairs = parts[1].split(",")
            for pair in param_pairs:
                key, value = pair.split("=")
                params[key.strip()] = value.strip()

        return method, params
