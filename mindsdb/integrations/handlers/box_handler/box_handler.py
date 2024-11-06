import io
import pandas as pd
from box_sdk_gen import BoxClient, BoxDeveloperTokenAuth
from typing import Dict, Optional, Text

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier, Insert

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


from mindsdb.integrations.libs.api_handler import APIHandler, APIResource


class ListFilesTable(APIResource):

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        files = self.handler._list_files()
        data = []
        for file in files:
            item = {
                "path": file["path"],
                "name": file["name"],
                "extension": file["extension"],
            }
            data.append(item)
        df = pd.DataFrame(data)
        return df

    def get_columns(self):
        return ["path", "name", "extension"]


class FileTable(APIResource):

    def _get_file_df(self):
        try:
            df = self.handler._read_file(self.table_name)
            if df is None:
                raise Exception(f"No such file found for the path: {self.dropbox_path}")

            return df
        except Exception as e:
            self.handler.logger.error(e)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        return self._get_file_df()

    def get_columns(self):
        df = self.handler._read_file(self.table_name)
        return df.columns.tolist()

    def insert(self, query: Insert) -> None:
        columns = [col.name for col in query.columns]
        data = [dict(zip(columns, row)) for row in query.values]
        df_new = pd.DataFrame(data)
        df_existing = self._get_file_df()
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        self.handler._write_file(self.table_name, df_combined)


class BoxHandler(APIHandler):

    name = "box"
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.logger = log.getLogger(__name__)
        self.client = None
        self.is_connected = False
        self._files_table = ListFilesTable(self)
        self._register_table("files", self._files_table)

    def connect(self):
        try:
            if self.is_connected:
                return
            if "token" not in self.connection_data:
                raise ValueError("Developer token must be provided.")
            auth = BoxDeveloperTokenAuth(token=self.connection_data["token"])
            self.client = BoxClient(auth=auth)
            self.is_connected = True
            self.logger.info("Connected to Box")
        except Exception as e:
            self.logger.error(f"Error connecting to Box: {e}")

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        try:
            self.connect()
            response.success = True
        except Exception as e:
            self.logger.error(f"Error with Box Handler: {e}")
            response.error_message = str(e)
        return response

    def disconnect(self):
        if not self.is_connected:
            return
        self.dbx = None
        self.is_connected = False
        self.logger.info("Disconnected from Box")
