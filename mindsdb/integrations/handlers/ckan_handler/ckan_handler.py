import pandas as pd
from ckanapi import RemoteCKAN
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse,
    HandlerResponse,
    RESPONSE_TYPE,
)
from mindsdb_sql.parser import ast
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DynamicResourceTable(APITable):
    """
    A class representing a CKAN resource table that is fetched dynamically
    """
    def __init__(self, handler, resource_id, resource_name):
        super().__init__(handler)
        self.resource_id = resource_id
        self.resource_name = resource_name
        self._fields = None

    @property
    def fields(self):
        if self._fields is None:
            self._fetch_fields()
        return self._fields

    def _fetch_fields(self):
        resource_info = self.handler.call_ckan_api(
            "datastore_search", {"resource_id": self.resource_id, "limit": 0}
        )
        self._fields = resource_info.get("fields", [])

    def select(self, query: ast.Select) -> HandlerResponse:
        params = {"resource_id": self.resource_id, "limit": 100}

        # Handle WHERE conditions
        if query.where:
            conditions = query.where.get_conditions()
            for op, arg1, arg2 in conditions:
                if op == "=":
                    params[arg1] = arg2
                elif op in (">", "<", ">=", "<="):
                    params[f"{arg1}{op}"] = arg2

        # Handle LIMIT clause
        if query.limit:
            params["limit"] = query.limit.value

        result = self.handler.call_ckan_api("datastore_search", params)

        # Collect the columns to return in the result
        columns = [
            target.parts[-1]
            for target in query.targets
            if isinstance(target, ast.Identifier)
        ]

        if not columns or "*" in columns:
            columns = result.columns

        return HandlerResponse(RESPONSE_TYPE.TABLE, result[columns])

    def get_columns(self):
        """
        Get the columns of the table for each resource
        """
        return [field["id"] for field in self.fields]


class CkanHandler(APIHandler):
    name = "ckan"

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.connection_args = kwargs.get("connection_data", {})
        self.tables = {}
        self.resource_metadata = None
        self.id_to_name = {}

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
            self._fetch_resource_metadata()
        except Exception as e:
            logger.error(f"Error connecting to CKAN: {e}")
            raise ConnectionError(f"Failed to connect to CKAN: {e}")

        return self.connection

    def _fetch_resource_metadata(self):
        if self.resource_metadata is None:
            try:
                metadata = self.call_ckan_api(
                    "datastore_search", {"resource_id": "_table_metadata"}
                )
                self.resource_metadata = metadata.to_dict("records")
                logger.debug(
                    f"Fetched metadata for {len(self.resource_metadata)} resources"
                )
                # Attempt to create id_to_name mapping
                for resource in self.resource_metadata:
                    id_field = resource.get("id") or resource.get("name")
                    name_field = resource.get("name") or resource.get("id")
                    if id_field and name_field:
                        self.id_to_name[id_field] = name_field

                logger.debug(
                    f"Created id_to_name mapping for {len(self.id_to_name)} resources"
                )
            except Exception as e:
                logger.error(f"Error fetching resource metadata: {e}")
                pass

    def _register_table(self, resource_id):
        """
        Register a table with the given resource ID
        """
        if resource_id not in self.tables:
            resource_name = self.id_to_name.get(resource_id, resource_id)
            table = DynamicResourceTable(self, resource_id, resource_name)
            super()._register_table(resource_id, table)
            self.tables[resource_id] = table
            self.tables[resource_name] = table
            logger.info(f"Registered table: {resource_name} (ID: {resource_id})")

    def check_connection(self) -> HandlerStatusResponse:
        try:
            self.connect()
            return HandlerStatusResponse(success=True)
        except Exception as e:
            return HandlerStatusResponse(success=False, error_message=str(e))

    def get_tables(self) -> HandlerResponse:
        self.connect()
        table_names = list(self.id_to_name.values())
        return HandlerResponse(
            RESPONSE_TYPE.TABLE, pd.DataFrame({"table_name": table_names})
        )

    def get_table(self, table_name):
        self.connect()

        logger.info(f"Attempting to get table: {table_name}")

        if isinstance(table_name, ast.Identifier):
            table_name = table_name.parts[-1]
        table_name = table_name.strip("`")

        if table_name in self.tables:
            return self.tables[table_name]

        # Check if table_name is a resource ID
        if table_name in self.id_to_name:
            self._register_table(table_name)
            return self.tables[table_name]

        # Check if table_name is a resource name
        for resource_id, resource_name in self.id_to_name.items():
            if resource_name == table_name:
                self._register_table(resource_id)
                return self.tables[resource_name]

        raise ValueError(f"Table {table_name} not found")

    def query(self, query: ast.Select) -> HandlerResponse:
        table = self.get_table(query.from_table)
        return table.select(query)

    def native_query(self, query: str = None) -> HandlerResponse:
        method, params = self.parse_native_query(query)
        df = self.call_ckan_api(method, params)
        return HandlerResponse(RESPONSE_TYPE.TABLE, df)

    def call_ckan_api(self, method_name: str, params: dict) -> pd.DataFrame:
        connection = self.connect()
        method = getattr(connection.action, method_name)

        try:
            result = method(**params)
            if "records" in result:
                df = pd.DataFrame(result["records"])
                return df
            else:
                return pd.DataFrame(result)
        except Exception as e:
            logger.error(f"Error calling CKAN API: {e}")
            raise RuntimeError(f"Failed to call CKAN API: {e}")

    @staticmethod
    def parse_native_query(query: str):
        parts = query.split(":")
        method = parts[0].strip()
        params = {}
        if len(parts) > 1:
            param_pairs = parts[1].split(",")
            for pair in param_pairs:
                key, value = pair.split("=")
                params[key.strip()] = value.strip()
        return method, params
