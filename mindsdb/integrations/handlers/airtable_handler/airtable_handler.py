from typing import Optional, List
import pandas as pd
import pyairtable

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import (
    extract_comparison_conditions,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from mindsdb_sql_parser import ast

from pyairtable.formulas import AND, OR, Field
from pyairtable.exceptions import PyAirtableError

logger = log.getLogger(__name__)


class AirtableTable(APITable):
    def __init__(self, handler, table_name: str):
        super().__init__(handler)
        self.table_name = table_name
        self.table = pyairtable.Table(
            self.handler.access_token, self.handler.base_id, self.table_name
        )

    def get_columns(self) -> List[str]:
        fields = self.handler.table_fields.get(self.table_name)
        if fields is None:
            schema = self.table.get_schema()
            fields = [field["name"] for field in schema["fields"]]
            self.handler.table_fields[self.table_name] = fields
        return fields

    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)
        formula = None
        for op, field_name, value in conditions:
            airtable_field = Field(field_name)
            if op == "=":
                condition = airtable_field == value
            elif op == "!=":
                condition = airtable_field != value
            elif op == ">":
                condition = airtable_field > value
            elif op == ">=":
                condition = airtable_field >= value
            elif op == "<":
                condition = airtable_field < value
            elif op == "<=":
                condition = airtable_field <= value
            elif op == "in":
                condition = OR(*(airtable_field == v for v in value))
            elif op == "not in":
                condition = AND(*(airtable_field != v for v in value))
            else:
                raise NotImplementedError(f"Operator {op} not supported")
            formula = condition if formula is None else AND(formula, condition)

        airtable_kwargs = {}
        if formula:
            airtable_kwargs["formula"] = str(formula)
        if query.limit:
            airtable_kwargs["max_records"] = int(query.limit.value)
        records = self.table.all(**airtable_kwargs)
        data = [record["fields"] for record in records]
        df = pd.DataFrame(data)

        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError(f"Unsupported target: {target}")

        if columns:
            df = df[columns]

        if query.order_by:
            sort_params = []
            for order in query.order_by:
                field_name = order.field.parts[-1]
                direction = "asc" if order.direction.lower() == "asc" else "desc"
                sort_params.append((field_name, direction))
            df = df.sort_values(by=[field_name], ascending=(direction == "asc"))

        return df

    def insert(self, query: ast.Insert):
        columns = [col.name for col in query.columns]
        data = [
            {col: value for col, value in zip(columns, row)} for row in query.values
        ]
        for record in data:
            self.table.create(record)


class AirtableHandler(APIHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = "airtable"

    def __init__(self, name: str, connection_data: Optional[dict] = None, **kwargs):
        super().__init__(name)

        self.connection_data = connection_data or {}
        self.access_token = self.connection_data.get("access_token")
        self.base_id = self.connection_data.get("base_id")
        self.is_connected = False
        self.api = None
        self.base = None
        self.table_fields = {}
        self._register_table("tables", self)

    def connect(self):
        if self.is_connected:
            return self.api
        try:
            self.api = pyairtable.Api(self.access_token)
            self.base = self.api.base(self.base_id)
            tables_metadata = self.base.tables()
            for table_meta in tables_metadata:
                table_name = table_meta.name
                self._register_table(
                    table_name, AirtableTable(self, table_name=table_name)
                )
            self.is_connected = True

        except PyAirtableError as e:
            logger.error(f"Error connecting to Airtable (PyAirtableError): {e}")
            self.is_connected = False
            raise e
        except Exception as e:
            logger.error(f"Error in Airtable handler: {e}")
            self.is_connected = False
            raise e
        return self.api

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Airtable: {e}")
            response.error_message = str(e)
            self.is_connected = False
        return response

    def query(self, query: ast.ASTNode) -> Response:
        if not self.is_connected:
            self.connect()
        if isinstance(query, ast.Select):
            table_name = query.from_table.parts[-1]
            if table_name == "tables":
                data = [
                    {"table_name": name}
                    for name in self._tables.keys()
                    if name != "tables"
                ]
                df = pd.DataFrame(data)
                return Response(RESPONSE_TYPE.TABLE, data_frame=df)
            table = self._get_table(query.from_table)
            df = table.select(query)
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        elif isinstance(query, ast.Insert):
            table = self._get_table(query.table)
            table.insert(query)
            return Response(RESPONSE_TYPE.OK)
        else:
            raise NotImplementedError(f"Query type {type(query)} is not supported.")

    def get_tables(self) -> Response:
        data = [
            {"table_name": name} for name in self._tables.keys() if name != "tables"
        ]
        df = pd.DataFrame(data)
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def get_columns(self, table_name: str) -> Response:
        table = self._tables.get(table_name)
        if table:
            columns = table.get_columns()
            data = [{"column_name": col} for col in columns]
            df = pd.DataFrame(data)
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        else:
            raise ValueError(f"Table {table_name} does not exist in Airtable.")
