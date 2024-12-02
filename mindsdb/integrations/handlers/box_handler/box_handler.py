import io
import pandas as pd
from box_sdk_gen import BoxClient, BoxDeveloperTokenAuth, CreateFolderParent, UploadFileAttributes, UploadFileAttributesParentField
from box_sdk_gen.internal import utils
from typing import Dict, Optional, Text

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier, Insert

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
                raise Exception(f"No such file found for the path: {self.box_path}")

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
            self.client.folders.get_folder_items("0", limit=1)
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
            self.logger.error(f"Error with Box Handler while establish connection: {e}")
            response.error_message = str(e)
        return response

    def disconnect(self):
        if not self.is_connected:
            return
        self.client = None
        self.is_connected = False
        self.logger.info("Disconnected from Box")

    def _read_as_content(self, file_path) -> utils.Buffer:
        """
        Read files as content
        """
        try:
            id = "0"  # Root folder id. The value is always "0".

            items = file_path.strip('/').split('/')

            for item in items:
                item_id = None
                for entry in self.client.folders.get_folder_items(id).entries:
                    if entry.name == item:
                        item_id = entry.id
                        break
                if item_id:
                    id = item_id
                else:
                    raise ValueError(f"{item} not found. Specify the correct path.")

            downloaded_file_content = self.client.downloads.download_file(id)
            buffer = utils.read_byte_stream(downloaded_file_content)
            return buffer
        except Exception as e:
            self.logger.error(f"Error when downloading a file from Box: {e}")

    def query(self, query: ASTNode) -> Response:

        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]
            if table_name == "files":
                table = self._files_table
                df = table.select(query)
                has_content = False
                for target in query.targets:
                    if (
                        isinstance(target, Identifier)
                        and target.parts[-1].lower() == "content"
                    ):
                        has_content = True
                        break
                if has_content:
                    df["content"] = df["path"].apply(self._read_as_content)
            else:
                table = FileTable(self, table_name=table_name)
                df = table.select(query)

            return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        elif isinstance(query, Insert):
            table_name = query.table.parts[-1]
            table = FileTable(self, table_name=table_name)
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

    def get_columns(self, table_name: str) -> Response:
        table = self._get_table(Identifier(table_name))
        columns = table.get_columns()
        df = pd.DataFrame(columns, columns=["column_name"])
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def list_files_in_folder(self, folder_id, files=[]):
        items = self.client.folders.get_folder_items(folder_id).entries
        for item in items:
            if item.type == 'folder':
                self.list_files_in_folder(item.id, files)
            elif item.type == 'file':
                extension = item.name.split(".")[-1].lower()
                if extension in self.supported_file_formats:
                    file = self.client.files.get_file_by_id(item.id)
                    full_path = [path.name for path in file.path_collection.entries][1:]
                    full_path.append(item.name)
                    files.append(
                        {
                            "path": "/".join(full_path),
                            "name": item.name,
                            "extension": extension,
                        }
                    )

        return files

    def _list_files(self, path=""):
        return self.list_files_in_folder("0")

    def _read_file(self, path) -> pd.DataFrame:
        try:
            buffer = self._read_as_content(path)
            extension = path.split(".")[-1].lower()
            if extension == "csv":
                df = pd.read_csv(io.BytesIO(buffer))
            elif extension == "tsv":
                df = pd.read_csv(io.BytesIO(buffer), sep="\t")
            elif extension == "json":
                df = pd.read_json(io.BytesIO(buffer))
            elif extension == "parquet":
                df = pd.read_parquet(io.BytesIO(buffer))
            else:
                raise ValueError(f"Unsupported file format: {extension}")
            return df
        except Exception as e:
            self.logger.error(f"Error with Box Handler while reading file: {e}")

    def upload_file(self, path, buffer):
        parent_folder_id = "0"

        directories = path.strip('/').split('/')

        for direc in directories[:-1]:
            sub_folder_id = None
            for item in self.client.folders.get_folder_items(parent_folder_id).entries:
                if item.type == "folder" and item.name == direc:
                    sub_folder_id = item.id
                    break
            if sub_folder_id:
                parent_folder_id = sub_folder_id
            else:
                new_sub_folder = self.client.folders.create_folder(direc, CreateFolderParent(id=parent_folder_id))
                parent_folder_id = new_sub_folder.id
                self.logger.debug(f"folder {direc} not available in path. Creating {direc}...")

        self.client.uploads.upload_file(UploadFileAttributes(name=directories[-1], parent=UploadFileAttributesParentField(id=parent_folder_id)), buffer)

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
        self.upload_file(path, buffer)
