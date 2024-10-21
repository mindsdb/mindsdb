import pandas as pd
from ckanapi import RemoteCKAN

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
    '''
    Datasets table contains information about CKAN datasets.
    This table is used to list all datasets available in CKAN that have datastore active resources.
    '''
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

    def get_columns(self):
        return [
            "id",
            "name",
            "title",
            "num_resources",
            "num_datastore_active_resources",
        ]


class ResourceIDsTable(APITable):
    '''
    ResourceIDs table contains information about CKAN resources.
    This table is used to list all resources available in CKAN that are datastore active.
    '''
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
    '''
    Datastore table is used to query CKAN datastore resources.
    This table is used to query data from CKAN datastore resources.
    It is using the datastore_search_sql API to execute SQL queries on CKAN datastore resources.
    '''
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where) if query.where else []
        resource_id = self.extract_resource_id(conditions)

        if resource_id:
            return self.execute_resource_query(query, resource_id)
        else:
            message = "Please provide a resource_id in your query. Example: SELECT * FROM datastore WHERE resource_id = 'your_resource_id'"
            df = pd.DataFrame({"message": [message]})
            return df

    def execute_resource_query(
        self, query: ast.Select, resource_id: str
    ) -> pd.DataFrame:
        sql_query = self.ast_to_sql(query, resource_id)
        result = self.handler.call_ckan_api("datastore_search_sql", {"sql": sql_query})

        records = result.get("records", [])

        df = pd.DataFrame(records)

        df = df.loc[:, ~df.columns.str.startswith("_")]

        return df

    def ast_to_sql(self, query: ast.Select, resource_id: str) -> str:
        sql_parts = [
            f"SELECT {self.render_columns(query.targets)}",
            f'FROM "{resource_id}"',
        ]

        # Handle WHERE clause
        where_conditions = []
        if query.where:
            where_conditions = self.extract_where_conditions(query.where)

        where_conditions = [
            cond
            for cond in where_conditions
            if not (
                isinstance(cond, ast.BinaryOperation)
                and cond.args[0].parts[-1] == "resource_id"
            )
        ]

        if where_conditions:
            sql_parts.append(
                f'WHERE {" AND ".join(self.render_where(cond) for cond in where_conditions)}'
            )

        # Handle LIMIT
        if query.limit:
            sql_parts.append(f"LIMIT {query.limit.value}")

        return " ".join(sql_parts)

    def render_columns(self, targets):
        if not targets or (len(targets) == 1 and isinstance(targets[0], ast.Star)):
            return "*"
        return ", ".join(self.render_column(target) for target in targets)

    def render_column(self, target):
        if isinstance(target, ast.Identifier):
            return f'"{target.parts[-1]}"'
        # Handle other types of targets as needed
        return str(target)

    def extract_where_conditions(self, where):
        if isinstance(where, ast.BinaryOperation) and where.op == "and":
            return self.extract_where_conditions(
                where.args[0]
            ) + self.extract_where_conditions(where.args[1])
        return [where]

    def render_where(self, where):
        if isinstance(where, ast.BinaryOperation):
            left = self.render_where(where.args[0])
            right = self.render_where(where.args[1])

            if where.op == "like":
                return f"{left} ILIKE {right}"
            elif where.op in ["=", ">", "<", ">=", "<=", "<>"]:
                return f"{left} {where.op} {right}"
            # Add more operators as needed

        elif isinstance(where, ast.Constant):
            return (
                f"'{where.value}'" if isinstance(where.value, str) else str(where.value)
            )

        elif isinstance(where, ast.Identifier):
            return f'"{where.parts[-1]}"'

        # Handle other types of WHERE conditions as needed
        return str(where)

    def extract_resource_id(self, conditions):
        for condition in conditions:
            if isinstance(condition, list) and len(condition) == 3:
                op, col, val = condition
                if col == "resource_id" and op == "=":
                    return val
        return None

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
