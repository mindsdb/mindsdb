import codecs
import csv
import json
import os
import shutil
import tempfile
import traceback
from io import BytesIO, StringIO
from pathlib import Path
from urllib.parse import urlparse

import filetype
import pandas as pd
import requests
from charset_normalizer import from_bytes
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import CreateTable, DropTables, Insert, Select
from mindsdb_sql.parser.ast.base import ASTNode
from langchain.text_splitter import RecursiveCharacterTextSplitter

from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log

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
            table_name = query.from_table.parts[-1]
            file_path = self.file_controller.get_file_path(table_name)
            df, _columns = self._handle_source(
                file_path,
                self.clean_rows,
                self.custom_parser,
                self.chunk_size,
                self.chunk_overlap,
            )
            result_df = query_df(df, query)
            return Response(RESPONSE_TYPE.TABLE, data_frame=result_df)

        elif type(query) is Insert:
            table_name = query.table.parts[-1]
            file_path = self.file_controller.get_file_path(table_name)

            # Load the existing data from the file
            df, _ = self._handle_source(
                file_path,
                self.clean_rows,
                self.custom_parser,
                self.chunk_size,
                self.chunk_overlap,
            )

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
        ast = self.parser(query, dialect="mindsdb")
        return self.query(ast)

    @staticmethod
    def _handle_source(
        file_path,
        clean_rows=True,
        custom_parser=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        chunk_overlap=DEFAULT_CHUNK_OVERLAP,
    ):
        """
        This function takes a file path and returns a pandas dataframe
        """
        # get file data io, format and dialect
        data, fmt, dialect = FileHandler._get_data_io(file_path)
        data.seek(0)  # make sure we are at 0 in file pointer

        if custom_parser:
            header, file_data = custom_parser(data, fmt)
            df = pd.DataFrame(file_data, columns=header)

        elif fmt == "parquet":
            df = pd.read_parquet(data)

        elif fmt == "csv":
            df = pd.read_csv(data, sep=dialect.delimiter, index_col=False)

        elif fmt in ["xlsx", "xls"]:
            data.seek(0)
            df = pd.read_excel(data)

        elif fmt == "json":
            data.seek(0)
            json_doc = json.loads(data.read())
            df = pd.json_normalize(json_doc, max_level=0)

        elif fmt == "txt" or fmt == "pdf":
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size, chunk_overlap=chunk_overlap
            )

            if fmt == "txt":
                try:
                    from langchain_community.document_loaders import TextLoader
                except ImportError:
                    raise ImportError(
                        "To import TXT document please install 'langchain-community':\n"
                        "    pip install langchain-community"
                    )
                loader = TextLoader(file_path, encoding="utf8")
                docs = text_splitter.split_documents(loader.load())
                df = pd.DataFrame(
                    [
                        {"content": doc.page_content, "metadata": doc.metadata}
                        for doc in docs
                    ]
                )

            elif fmt == "pdf":

                import fitz  # pymupdf

                with fitz.open(file_path) as pdf:  # open pdf
                    text = chr(12).join([page.get_text() for page in pdf])

                split_text = text_splitter.split_text(text)

                df = pd.DataFrame(
                    {"content": split_text, "metadata": [{}] * len(split_text)}
                )

        else:
            raise ValueError(
                "Could not load file into any format, supported formats are csv, json, xls, xlsx, pdf, txt"
            )

        header = df.columns.values.tolist()

        df = df.rename(columns={key: key.strip() for key in header})
        df = df.applymap(clean_cell)

        header = [x.strip() for x in header]
        col_map = dict((col, col) for col in header)
        return df, col_map

    @staticmethod
    def is_it_parquet(data: BytesIO) -> bool:
        # Check first and last 4 bytes equal to PAR1.
        # Refer: https://parquet.apache.org/docs/file-format/
        parquet_sig = b"PAR1"
        data.seek(0, 0)
        start_meta = data.read(4)
        data.seek(-4, 2)
        end_meta = data.read()
        data.seek(0)
        if start_meta == parquet_sig and end_meta == parquet_sig:
            return True
        return False

    @staticmethod
    def is_it_xlsx(file_path: str) -> bool:
        file_type = filetype.guess(file_path)
        if file_type and file_type.mime in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        }:
            return True
        return False

    @staticmethod
    def is_it_json(data_str: StringIO) -> bool:
        # see if its JSON
        text = data_str.read(100).strip()
        data_str.seek(0)
        if len(text) > 0:
            # it it looks like a json, then try to parse it
            if text.startswith("{") or text.startswith("["):
                try:
                    json.loads(data_str.read())
                    return True
                except Exception:
                    return False
                finally:
                    data_str.seek(0)
        return False

    @staticmethod
    def is_it_csv(data_str: StringIO) -> bool:
        sample = data_str.readline()  # trying to get dialect from header
        data_str.seek(0)
        try:
            csv.Sniffer().sniff(sample)
            # Avoid a false-positive for json files
            try:
                json.loads(data_str.read())
                data_str.seek(0)
                return False
            except json.decoder.JSONDecodeError:
                data_str.seek(0)
                return True
        except Exception:
            return False

    @staticmethod
    def _get_data_io(file_path):
        """
        @TODO: Use python-magic to simplify the function and detect the file types as the xlsx example
        This gets a file either url or local file and defines what the format is as well as dialect
        :param file: file path or url
        :return: data_io, format, dialect
        """

        data = BytesIO()
        data_str = None
        dialect = None

        try:
            with open(file_path, "rb") as fp:
                data = BytesIO(fp.read())
        except Exception as e:
            error = "Could not load file, possible exception : {exception}".format(
                exception=e
            )
            logger.error(error)
            raise ValueError(error)

        suffix = Path(file_path).suffix.strip(".").lower()
        if suffix not in ("csv", "json", "xlsx", "parquet"):
            if FileHandler.is_it_parquet(data):
                suffix = "parquet"
            elif FileHandler.is_it_xlsx(file_path):
                suffix = "xlsx"

        if suffix == "parquet":
            return data, "parquet", dialect

        if suffix == "xlsx":
            return data, "xlsx", dialect

        if suffix == "txt":
            return data, "txt", dialect

        if suffix == "pdf":
            return data, "pdf", dialect

        byte_str = data.read()
        # Move it to StringIO
        try:
            # Handle Microsoft's BOM "special" UTF-8 encoding
            if byte_str.startswith(codecs.BOM_UTF8):
                data_str = StringIO(byte_str.decode("utf-8-sig"))
            else:
                file_encoding_meta = from_bytes(
                    byte_str[: 32 * 1024],
                    steps=32,  # Number of steps/block to extract from my_byte_str
                    chunk_size=1024,  # Set block size of each extraction)
                    explain=False,
                )
                best_meta = file_encoding_meta.best()
                errors = "strict"
                if best_meta is not None:
                    encoding = file_encoding_meta.best().encoding

                    try:
                        data_str = StringIO(byte_str.decode(encoding, errors))
                    except UnicodeDecodeError:
                        encoding = "utf-8"
                        errors = "replace"

                        data_str = StringIO(byte_str.decode(encoding, errors))
                else:
                    encoding = "utf-8"
                    errors = "replace"

                    data_str = StringIO(byte_str.decode(encoding, errors))
        except Exception:
            logger.error(traceback.format_exc())
            logger.error("Could not load into string")

        if suffix not in ("csv", "json"):
            if FileHandler.is_it_json(data_str):
                suffix = "json"
            elif FileHandler.is_it_csv(data_str):
                suffix = "csv"

        if suffix == "json":
            return data_str, suffix, dialect

        if suffix == "csv":
            try:
                dialect = FileHandler._get_csv_dialect(data_str)
                if dialect:
                    return data_str, "csv", dialect
            except Exception:
                logger.error("Could not detect format for this file")
                logger.error(traceback.format_exc())

        data_str.seek(0)
        data.seek(0)

        # No file type identified
        return data, None, dialect

    @staticmethod
    def _get_file_path(path) -> str:
        try:
            is_url = urlparse(path).scheme in ("http", "https")
        except Exception:
            is_url = False
        if is_url:
            path = FileHandler._fetch_url(path)
        return path

    @staticmethod
    def _get_csv_dialect(buffer) -> csv.Dialect:
        sample = buffer.readline()  # trying to get dialect from header
        buffer.seek(0)
        try:
            if isinstance(sample, bytes):
                sample = sample.decode()
            accepted_csv_delimiters = [",", "\t", ";"]
            try:
                dialect = csv.Sniffer().sniff(
                    sample, delimiters=accepted_csv_delimiters
                )
                dialect.doublequote = (
                    True  # assume that all csvs have " as string escape
                )
            except Exception:
                dialect = csv.reader(sample).dialect
                if dialect.delimiter not in accepted_csv_delimiters:
                    raise Exception(
                        f"CSV delimeter '{dialect.delimiter}' is not supported"
                    )

        except csv.Error:
            dialect = None
        return dialect

    @staticmethod
    def _fetch_url(url: str) -> str:
        temp_dir = tempfile.mkdtemp(prefix="mindsdb_file_url_")
        try:
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                with open(os.path.join(temp_dir, "file"), "wb") as f:
                    for chunk in r:
                        f.write(chunk)
            else:
                raise Exception(f"Response status code is {r.status_code}")
        except Exception as e:
            logger.error(f"Error during getting {url}")
            logger.error(e)
            raise
        return os.path.join(temp_dir, "file")

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
