import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import CreateTable, DropTables, Insert, Select
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log

from mindsdb.integrations.utilities.files.file_reader import FileReader


logger = log.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 250


def clean_cell(val):
    if str(val) in ["", " ", "  ", "NaN", "nan", "NA"]:
        return None
    return val


class FileHandler(DatabaseHandler):
    """
    Handler for files
    """

    name = "files"

    def __init__(
        self,
        name=None,
        file_storage=None,
        connection_data={},
        file_controller=None,
        **kwargs,
    ):
        super().__init__(name)
        self.parser = parse_sql
        self.fs_store = file_storage
        self.custom_parser = connection_data.get("custom_parser", None)
        self.clean_rows = connection_data.get("clean_rows", True)
        self.chunk_size = connection_data.get("chunk_size", DEFAULT_CHUNK_SIZE)
        self.chunk_overlap = connection_data.get("chunk_overlap", DEFAULT_CHUNK_OVERLAP)
        self.file_controller = file_controller

    def connect(self, **kwargs):
        return

    def disconnect(self, **kwargs):
        return

    def check_connection(self) -> StatusResponse:
        return StatusResponse(True)

    def query(self, query: ASTNode) -> Response:
        if type(query) is DropTables:
            for table_identifier in query.tables:
                if (
                    len(table_identifier.parts) == 2
                    and table_identifier.parts[0] != self.name
                ):
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table from database '{table_identifier.parts[0]}'",
                    )
                table_name = table_identifier.parts[-1]
                try:
                    self.file_controller.delete_file(table_name)
                except Exception as e:
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table '{table_name}': {e}",
                    )
            return Response(RESPONSE_TYPE.OK)

        if type(query) is CreateTable:
            # Check if the table already exists or if the table name contains more than one namespace
            existing_files = self.file_controller.get_files_names()

            if len(query.name.parts) != 1:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message="Table name cannot contain more than one namespace",
                )

            table_name = query.name.parts[-1]
            if table_name in existing_files:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Table '{table_name}' already exists",
                )

            if query.is_replace:
                self.file_controller.delete_file(table_name)

            temp_dir_path = tempfile.mkdtemp(prefix="mindsdb_file_")

            try:
                # Create a temp file to save the table
                temp_file_path = os.path.join(temp_dir_path, f"{table_name}.csv")

                # Create an empty file using with the columns in the query
                df = pd.DataFrame(columns=[col.name for col in query.columns])
                df.to_csv(temp_file_path, index=False)

                self.file_controller.save_file(table_name, temp_file_path, file_name=f"{table_name}.csv")
            except Exception as unknown_error:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Error creating table '{table_name}': {unknown_error}",
                )
            finally:
                # Remove the temp dir created
                shutil.rmtree(temp_dir_path, ignore_errors=True)

            return Response(RESPONSE_TYPE.OK)

        elif type(query) is Select:
            table_name_parts = query.from_table.parts
            table_name = table_name_parts[-1]

            # Check if it's a multi-part name (e.g., `files.file_name.sheet_name`)
            if len(table_name_parts) > 1:
                table_name = table_name_parts[-2]
                sheet_name = table_name_parts[-1]  # Get the sheet name
            else:
                sheet_name = None
            file_path = self.file_controller.get_file_path(table_name)

            df = self.handle_source(file_path, sheet_name=sheet_name)

            # Process the SELECT query
            result_df = query_df(df, query)
            return Response(RESPONSE_TYPE.TABLE, data_frame=result_df)

        elif type(query) is Insert:
            table_name = query.table.parts[-1]
            file_path = self.file_controller.get_file_path(table_name)

            file_reader = FileReader(path=file_path)

            df = file_reader.to_df()

            # Create a new dataframe with the values from the query
            new_df = pd.DataFrame(query.values, columns=[col.name for col in query.columns])

            # Concatenate the new dataframe with the existing one
            df = pd.concat([df, new_df], ignore_index=True)

            # Write the concatenated data to the file based on its format
            format = Path(file_path).suffix.strip(".").lower()
            write_method = getattr(df, f"to_{format}")
            write_method(file_path, index=False)

            return Response(RESPONSE_TYPE.OK)

        else:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message="Only 'select', 'insert', 'create' and 'drop' queries allowed for files",
            )

    def native_query(self, query: str) -> Response:
        ast = self.parser(query)
        return self.query(ast)

    @staticmethod
    def handle_source(file_path, **kwargs):
        file_reader = FileReader(path=file_path)

        df = file_reader.to_df(**kwargs)

        header = df.columns.values.tolist()

        df.columns = [key.strip() for key in header]
        df = df.applymap(clean_cell)
        return df

    def get_tables(self) -> Response:
        """
        List all files
        """
        files_meta = self.file_controller.get_files()
        data = [
            {
                "TABLE_NAME": x["name"],
                "TABLE_ROWS": x["row_count"],
                "TABLE_TYPE": "BASE TABLE",
            }
            for x in files_meta
        ]
        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))

    def get_columns(self, table_name) -> Response:
        file_meta = self.file_controller.get_file_meta(table_name)
        result = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                [
                    {
                        "Field": x["name"].strip()
                        if isinstance(x, dict)
                        else x.strip(),
                        "Type": "str",
                    }
                    for x in file_meta["columns"]
                ]
            ),
        )
        return result
