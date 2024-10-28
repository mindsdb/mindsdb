import io
import pandas as pd
import dropbox
import duckdb

from dropbox.exceptions import AuthError, ApiError
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
    def __init__(self, handler=None, table_name=None, dropbox_path=None):
        super().__init__(handler, table_name=table_name)
        self.dropbox_path = dropbox_path

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        df = self.handler._read_file(self.dropbox_path)
        return df

    def get_columns(self):
        df = self.handler._read_file(self.table_name)
        return df.columns.tolist()

    def insert(self, query: Insert) -> None:
        columns = [col.name for col in query.columns]
        data = [dict(zip(columns, row)) for row in query.values]
        df_new = pd.DataFrame(data)
        df_existing = self.handler._read_file(self.dropbox_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        self.handler._write_file(self.dropbox_path, df_combined)


class DropboxHandler(APIHandler):

    name = "dropbox"
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.logger = log.getLogger(__name__)
        self.dbx = None
        self.is_connected = False
        self.duckdb_conn = None
        self._register_table("files", ListFilesTable(self))

    def connect(self):
        if self.is_connected:
            return
        if "access_token" not in self.connection_data:
            raise ValueError("Access token must be provided.")
        self.dbx = dropbox.Dropbox(self.connection_data["access_token"])
        self.is_connected = True
        self.duckdb_con = duckdb.connect(database=":memory:")
        self.logger.info(
            f"Connected to Dropbox as {self.dbx.users_get_current_account().email}"
        )

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        try:
            self.connect()
            response.success = True
        except (AuthError, ApiError, ValueError) as e:
            self.logger.error(f"Error connecting to Dropbox: {e}")
            response.error_message = str(e)
        return response

    def disconnect(self):
        if not self.is_connected:
            return
        self.dbx = None
        self.is_connected = False
        self.logger.info("Disconnected from Dropbox")

    def query(self, query: ASTNode) -> Response:
        if isinstance(query, Select):
            table = self._get_table(query.from_table)
            result = table.select(query)
            return Response(RESPONSE_TYPE.TABLE, data_frame=result)
        elif isinstance(query, Insert):
            table = self._get_table(query.table)
            table.insert(query)
            return Response(RESPONSE_TYPE.OK)
        else:
            raise NotImplementedError(
                "Only SELECT and INSERT operations are supported."
            )

    def get_tables(self) -> Response:
        table_names = list(self._tables.keys())
        df = pd.DataFrame(table_names, columns=["table_name"])
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def _get_table(self, name: Identifier):
        table_name = name.parts[-1]
        if table_name not in self._tables:
            files = self._list_files()
            file_dict = {}
            for file in files:
                if file["name"] not in file_dict:
                    file_dict[file["name"]] = file
            if table_name in file_dict:
                self._register_table(
                    table_name,
                    FileTable(
                        self,
                        table_name=table_name,
                        dropbox_path=file_dict[table_name]["path"],
                    ),
                )
            else:
                raise Exception(f"Table not found: {table_name}")
        return self._tables[table_name]

    def get_columns(self, table_name: str) -> Response:
        table = self._get_table(Identifier(table_name))
        columns = table.get_columns()
        df = pd.DataFrame(columns, columns=["column_name"])
        df["data_type"] = "string"
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def _list_files(self, path=""):
        files = []
        result = self.dbx.files_list_folder(path, recursive=True)
        files.extend(self._process_entries(result.entries))
        while result.has_more:
            result = self.dbx.files_list_folder_continue(result.cursor)
            files.extend(self._process_entries(result.entries))
        return files

    def _process_entries(self, entries):
        files = []
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                extension = entry.name.split(".")[-1].lower()
                if extension in self.supported_file_formats:
                    files.append(
                        {
                            "path": entry.path_lower,
                            "name": entry.name,
                            "extension": extension,
                        }
                    )
        return files

    def _read_file(self, path) -> pd.DataFrame:
        _, res = self.dbx.files_download(path)
        content = res.content
        extension = path.split(".")[-1].lower()
        print(f"Content: {content}")
        if extension == "csv":
            df = pd.read_csv(io.BytesIO(content))
        elif extension == "tsv":
            df = pd.read_csv(io.BytesIO(content), sep="\t")
        elif extension == "json":
            df = pd.read_json(io.BytesIO(content))
            data_df = duckdb.read_json(content)
            print(f"Duckdb df: {data_df}")
        elif extension == "parquet":
            df = pd.read_parquet(io.BytesIO(content))
        else:
            raise ValueError(f"Unsupported file format: {extension}")
        return df

    def _write_file(self, path, df: pd.DataFrame):
        extension = path.split(".")[-1].lower()
        buffer = io.BytesIO()
        if extension == "csv":
            df.to_csv(buffer, index=False)
        elif extension == "tsv":
            df.to_csv(buffer, index=False, sep="\t")
        elif extension == "json":
            df.to_json(buffer, orient="records")
        elif extension == "parquet":
            df.to_parquet(buffer, index=False)
        else:
            raise ValueError(f"Unsupported file format: {extension}")
        buffer.seek(0)
        self.dbx.files_upload(
            buffer.read(), path, mode=dropbox.files.WriteMode.overwrite
        )
