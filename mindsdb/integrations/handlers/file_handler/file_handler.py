from io import BytesIO, StringIO
import os
import csv
import json
import codecs
import traceback
import tempfile
from urllib.parse import urlparse

import requests
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import DropTables, Select

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


def clean_row(row):
    n_row = []
    for cell in row:
        if str(cell) in ['', ' ', '  ', 'NaN', 'nan', 'NA']:
            n_row.append(None)
        else:
            n_row.append(cell)

    return n_row


class FileHandler(DatabaseHandler):
    """
    Handler for files
    """
    name = 'files'

    def __init__(self, name=None, db_store=None, fs_store=None, connection_data=None, file_controller=None):
        super().__init__(name)
        self.parser = parse_sql
        self.fs_store = fs_store
        self.custom_parser = connection_data.get('custom_parser')
        self.clean_rows = connection_data.get('clean_rows', True)
        self.file_controller = file_controller

    def connect(self, **kwargs):
        return

    def disconnect(self, **kwargs):
        return

    def check_connection(self) -> StatusResponse:
        return StatusResponse(True)

    def query(self, query: ASTNode) -> Response:
        if type(query) == DropTables:
            for table_identifier in query.tables:
                if len(table_identifier.parts) == 2 and table_identifier.parts[0] != self.name:
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table from database '{table_identifier.parts[0]}'"
                    )
                table_name = table_identifier.parts[-1]
                try:
                    self.file_controller.delete_file(table_name)
                except Exception as e:
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table '{table_name}': {e}"
                    )
            return Response(RESPONSE_TYPE.OK)
        elif type(query) == Select:
            table_name = query.from_table.parts[-1]
            file_path = self.file_controller.get_file_path(table_name)
            df, _columns = self._handle_source(file_path, self.clean_rows, self.custom_parser)
            result_df = query_df(df, query)
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=result_df
            )
        else:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message="Only 'select' and 'drop' queries allowed for files"
            )

    def native_query(self, query: str) -> Response:
        ast = self.parser(query, dialect='mindsdb')
        return self.query(ast)

    @staticmethod
    def _handle_source(file_path, clean_rows=True, custom_parser=None):
        # get file data io, format and dialect
        data, fmt, dialect = FileHandler._get_data_io(file_path)
        data.seek(0)  # make sure we are at 0 in file pointer

        if custom_parser:
            header, file_data = custom_parser(data, fmt)

        elif fmt == 'csv':
            csv_reader = list(csv.reader(data, dialect))
            header = csv_reader[0]
            file_data = csv_reader[1:]

        elif fmt in ['xlsx', 'xls']:
            data.seek(0)
            df = pd.read_excel(data)
            header = df.columns.values.tolist()
            file_data = df.values.tolist()

        elif fmt == 'json':
            data.seek(0)
            json_doc = json.loads(data.read())
            df = pd.json_normalize(json_doc, max_level=0)
            header = df.columns.values.tolist()
            file_data = df.values.tolist()

        else:
            raise ValueError('Could not load file into any format, supported formats are csv, json, xls, xlsx')

        if clean_rows:
            file_list_data = [clean_row(row) for row in file_data]
        else:
            file_list_data = file_data

        header = [x.strip() for x in header]
        col_map = dict((col, col) for col in header)
        return pd.DataFrame(file_list_data, columns=header), col_map

    @staticmethod
    def _get_data_io(file_path):
        """
        This gets a file either url or local file and defiens what the format is as well as dialect
        :param file: file path or url
        :return: data_io, format, dialect
        """

        ############
        # get file as io object
        ############

        # file_path = self._get_file_path()

        data = BytesIO()

        try:
            with open(file_path, 'rb') as fp:
                data = BytesIO(fp.read())
        except Exception as e:
            error = 'Could not load file, possible exception : {exception}'.format(exception=e)
            print(error)
            raise ValueError(error)

        dialect = None

        ############
        # check for file type
        ############

        # try to guess if its an excel file
        xlsx_sig = b'\x50\x4B\x05\06'
        # xlsx_sig2 = b'\x50\x4B\x03\x04'
        xls_sig = b'\x09\x08\x10\x00\x00\x06\x05\x00'

        # different whence, offset, size for different types
        excel_meta = [('xls', 0, 512, 8), ('xlsx', 2, -22, 4)]

        for filename, whence, offset, size in excel_meta:

            try:
                data.seek(offset, whence)  # Seek to the offset.
                bytes = data.read(size)  # Capture the specified number of bytes.
                data.seek(0)
                codecs.getencoder('hex')(bytes)

                if bytes == xls_sig:
                    return data, 'xls', dialect
                elif bytes == xlsx_sig:
                    return data, 'xlsx', dialect

            except Exception:
                data.seek(0)

        # if not excel it can be a json file or a CSV, convert from binary to stringio

        byte_str = data.read()
        # Move it to StringIO
        try:
            # Handle Microsoft's BOM "special" UTF-8 encoding
            if byte_str.startswith(codecs.BOM_UTF8):
                data = StringIO(byte_str.decode('utf-8-sig'))
            else:
                data = StringIO(byte_str.decode('utf-8'))

        except Exception:
            print(traceback.format_exc())
            print('Could not load into string')

        # see if its JSON
        buffer = data.read(100)
        data.seek(0)
        text = buffer.strip()
        # analyze first n characters
        if len(text) > 0:
            text = text.strip()
            # it it looks like a json, then try to parse it
            if text.startswith('{') or text.startswith('['):
                try:
                    json.loads(data.read())
                    data.seek(0)
                    return data, 'json', dialect
                except Exception:
                    data.seek(0)
                    return data, None, dialect

        # lets try to figure out if its a csv
        try:
            dialect = FileHandler._get_csv_dialect(file_path)
            if dialect:
                return data, 'csv', dialect
            return data, None, dialect
        except Exception:
            data.seek(0)
            print('Could not detect format for this file')
            print(traceback.format_exc())
            # No file type identified
            return data, None, dialect

    @staticmethod
    def _get_file_path(path) -> str:
        try:
            is_url = urlparse(path).scheme in ('http', 'https')
        except Exception:
            is_url = False
        if is_url:
            path = FileHandler._fetch_url(path)
        return path

    @staticmethod
    def _get_csv_dialect(file_path) -> csv.Dialect:
        with open(file_path, 'rt') as f:
            try:
                accepted_csv_delimiters = [',', '\t', ';']
                dialect = csv.Sniffer().sniff(f.read(128 * 1024), delimiters=accepted_csv_delimiters)
            except csv.Error:
                dialect = None
        return dialect

    @staticmethod
    def _fetch_url(url: str) -> str:
        temp_dir = tempfile.mkdtemp(prefix='mindsdb_file_url_')
        try:
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                with open(os.path.join(temp_dir, 'file'), 'wb') as f:
                    for chunk in r:
                        f.write(chunk)
            else:
                raise Exception(f'Responce status code is {r.status_code}')
        except Exception as e:
            print(f'Error during getting {url}')
            print(e)
            raise
        return os.path.join(temp_dir, 'file')

    def get_tables(self) -> Response:
        """
        List all files
        """
        files_meta = self.file_controller.get_files()
        data = [{
            'TABLE_NAME': x['name'],
            'TABLE_ROWS': x['row_count']
        } for x in files_meta]
        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(data)
        )

    def get_columns(self, table_name) -> Response:
        file_meta = self.file_controller.get_file_meta(table_name)
        result = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame([
                {
                    'Field': x.strip(),
                    'Type': 'str'
                } for x in file_meta['columns']
            ])
        )
        return result
