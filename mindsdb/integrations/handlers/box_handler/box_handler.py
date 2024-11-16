import io
import pandas as pd
from boxsdk import Client, OAuth2
from boxsdk.exception import BoxAPIException
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
                raise Exception(f"No such file found for the path: {self.table_name}")

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
            if "client_id" not in self.connection_data or "client_secret" not in self.connection_data:
                raise ValueError("Client ID and Client Secret must be provided.")
            oauth2 = OAuth2(
                client_id=self.connection_data["client_id"],
                client_secret=self.connection_data["client_secret"],
                access_token=self.connection_data.get("access_token"),
                refresh_token=self.connection_data.get("refresh_token"),
            )
            self.client = Client(oauth2)
            self.is_connected = True
            self.logger.info("Connected to Box")
        except ValueError as e:
            self.logger.error(f"Error connecting to Box: {e}")
        except BoxAPIException as e:
            self.logger.error(f"Box API error: {e}")
        except Exception as e:
            self.logger.error(f"Error with Box: {e}")

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        try:
            self.connect()
            response.success = True
        except (BoxAPIException, ValueError) as e:
            self.logger.error(f"Error connecting to Box: {e}")
            response.error_message = str(e)
        except Exception as e:
            self.logger.error(f"Error with Box Handler: {e}")
            response.error_message = str(e)
        return response

    def disconnect(self):
        if not self.is_connected:
            return
        self.client = None
        self.is_connected = False
        self.logger.info("Disconnected from Box")

    def _read_as_content(self, file_id) -> None:
        """
        Read files as content
        """
        try:
            file = self.client.file(file_id).content()
            return file
        except BoxAPIException as e:
            self.logger.error(f"Error when downloading a file from Box: {e}")

    def query(self, query: ASTNode) -> Response:

        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]
            if table_name == "files":
                table = self._files_table
                df = table.select(query)

                # add content
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

    def _list_files(self, folder_id="0"):
        files = []
        items = self.client.folder(folder_id).get_items()
        for item in items:
            if item.type == "file":
                extension = item.name.split(".")[-1].lower()
                if extension in self.supported_file_formats:
                    files.append(
                        {
                            "path": item.id,
                            "name": item.name,
                            "extension": extension,
                        }
                    )
        return files

    def _read_file(self, file_id) -> pd.DataFrame:
        try:
            file_content = self.client.file(file_id).content()
            extension = file_id.split(".")[-1].lower()
            if extension == "csv":
                df = pd.read_csv(io.BytesIO(file_content))
            elif extension == "tsv":
                df = pd.read_csv(io.BytesIO(file_content), sep="\t")
            elif extension == "json":
                df = pd.read_json(io.BytesIO(file_content))
            elif extension == "parquet":
                df = pd.read_parquet(io.BytesIO(file_content))
            else:
                raise ValueError(f"Unsupported file format: {extension}")
            return df
        except ValueError as e:
            self.logger.error(f"Error with file extension: {e}")
        except BoxAPIException as e:
            self.logger.error(f"Error when downloading a file from Box: {e}")
        except Exception as e:
            self.logger.error(f"Error with Box Handler: {e}")

    def _write_file(self, file_id, df: pd.DataFrame):
        try:
            extension = file_id.split(".")[-1].lower()
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
            self.client.file(file_id).update_contents(buffer)
        except ValueError as e:
            self.logger.error(f"Error with file extension: {e}")
        except BoxAPIException as e:
            self.logger.error(f"Error when writing a file to Box: {e}")
        except Exception as e:
            self.logger.error(f"Error with Box Handler: {e}")
