from typing import Optional

import pandas as pd

from pyairtable import Api

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

logger = log.getLogger(__name__)


class ListTables(APIResource):
    def list(self, targets=None, conditions=None, limit=None, **kwargs):
        data = []
        for table in self.handler.tables_metadata:
            columns = ", ".join([field["name"] for field in table.fields])
            data.append({"table_name": table.name, "columns": columns})
        df = pd.DataFrame(data)
        return df


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
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = "airtable"

    def __init__(self, name: str, connection_data: Optional[dict] = None, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.api_key = self.connection_data.get("api_key")
        self.base_id = self.connection_data.get("base_id")
        self.is_connected = False
        self.api = None
        self.base = None
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
            raise e
        return self.connection
