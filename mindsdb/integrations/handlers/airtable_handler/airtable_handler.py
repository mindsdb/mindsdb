from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.utilities import log
from pyairtable import Api
import pandas as pd
from typing import Optional, Dict
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


class ListTables(APIResource):
    def list(self, targets=None, conditions=None, limit=None, **kwargs):
        data = []
        for table in self.handler.tables_metadata:
            columns = ", ".join([field["name"] for field in table.fields])
            data.append({"table_name": table.name, "columns": columns})
        df = pd.DataFrame(data)
        return df

    def get_columns(self):
        return ["table_name", "columns"]


class AirtableTable(APIResource):
    def list(self, targets=None, conditions=None, limit=None, **kwargs):
        if not self.handler.is_connected:
            self.handler.connect()
        records = self.handler.call_application_api(
            "all", table_name=self.table_name, limit=limit
        )
        df = pd.DataFrame.from_records([record["fields"] for record in records])
        return df

    def insert(self, query):
        data = [
            {col.name: value for col, value in zip(query.columns, row)}
            for row in query.values
        ]
        for record in data:
            self.handler.api.table(self.handler.base_id, self.table_name).create(record)


class AirtableHandler(APIHandler):
    name = "airtable"

    def __init__(self, name: str, connection_data: Optional[Dict] = None, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.api_key = self.connection_data.get("api_key")
        self.base_id = self.connection_data.get("base_id")
        self.is_connected = False
        self.logger = log.getLogger(__name__)
        self._tables = {}
        self._register_table("tables", ListTables(self))

    def connect(self):
        if self.is_connected:
            return self.connection
        try:
            self.api = Api(self.api_key)
            self.base = self.api.base(self.base_id)
            self.tables_metadata = self.base.tables()
            for table in self.tables_metadata:
                table_name = table.name
                self._register_table(
                    table_name, AirtableTable(self, table_name=table_name)
                )
            self.is_connected = True
        except Exception as e:
            self.is_connected = False
            self.logger.error(f"Error with connecting to Airtable: {e}")
        return self.connection

    def disconnect(self):
        if not self.is_connected:
            return
        self.api = None
        self.base = None
        self.is_connected = False
        self.logger.info("Disconnected from Airtable.")

    def check_connection(self) -> StatusResponse:
        try:
            self.connect()
            if self.base:
                return StatusResponse(success=True)
            else:
                return StatusResponse(
                    success=False, error_message="Unable to connect to Airtable base."
                )
        except Exception as e:
            self.logger.error(f"Error checking connection to Airtable: {e}")
            return StatusResponse(success=False, error_message=str(e))

    def call_application_api(
        self,
        method_name: str,
        table_name: str = None,
        formula: str = None,
        limit: int = None,
        **kwargs,
    ):
        if not self.is_connected:
            self.connect()
        table = self.api.table(self.base_id, table_name)
        try:
            if method_name == "all":
                records = table.all(formula=formula, max_records=limit)
                return records
            else:
                raise NotImplementedError(f"Method '{method_name}' is not implemented.")
        except Exception as e:
            self.logger.error(f"Error calling an Airtable API: {e}")
