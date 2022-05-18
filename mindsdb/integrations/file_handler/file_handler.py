from io import BytesIO, StringIO
import os
import csv
import json
import codecs
import traceback
import tempfile

import requests
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.utilities.log import log
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


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

    def __init__(self, name, db_store=None, fs_store=None, connection_data=None):
        super().__init__(name)
        self.parser = parse_sql
        self.fs_store = fs_store    # or data_store ???
        self.custom_parser = connection_data.get('custom_parser')
        self.clean_rows = connection_data.get('clean_rows', True)

    def check_status(self):
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        return {
            'success': True
        }

    def query(self, query: ASTNode):
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        table_name = query.from_table.parts[-1]

        data_store = DataStore()    # TODO for cloud
        file_path = data_store.get_file_path(table_name, company_id=None)

        df, _columns = self._handle_source(file_path)
        result_df = query_df(df, query)

        return {
            'type': RESPONSE_TYPE.TABLE,
            'data_frame': result_df
        }

    def native_query(self, query):
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in PostgreSQL
        :return: returns the records from the current recordset
        """
        pass
        # TODO

    def _handle_source(self, file_path):
        self._file_name = os.path.basename(file_path)

        # get file data io, format and dialect
        data, fmt, self.dialect = self._getDataIo(file_path)
        data.seek(0)  # make sure we are at 0 in file pointer

        if self.custom_parser:
            header, file_data = self.custom_parser(data, fmt)

        elif fmt == 'csv':
            csv_reader = list(csv.reader(data, self.dialect))
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

        if self.clean_rows:
            file_list_data = [clean_row(row) for row in file_data]
        else:
            file_list_data = file_data

        col_map = dict((col, col) for col in header)
        return pd.DataFrame(file_list_data, columns=header), col_map

    def _getDataIo(self, file_path):
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
            dialect = self._get_csv_dialect(file_path)
            if dialect:
                return data, 'csv', dialect
            return data, None, dialect
        except Exception:
            data.seek(0)
            print('Could not detect format for this file')
            print(traceback.format_exc())
            # No file type identified
            return data, None, dialect

    def _get_file_path(self) -> str:
        path = self.file
        if self.is_url:
            self._fetch_url()
            path = os.path.join(self._temp_dir, 'file')
        return path

    def _get_csv_dialect(self, file_path) -> object:
        # file_path = self._get_file_path()
        with open(file_path, 'rt') as f:
            try:
                accepted_csv_delimiters = [',', '\t', ';']
                dialect = csv.Sniffer().sniff(f.read(128 * 1024), delimiters=accepted_csv_delimiters)
            except csv.Error:
                dialect = None
        return dialect

    def _fetch_url(self):
        if self._temp_dir is not None and os.path.isfile(os.path.join(self._temp_dir, 'file')):
            return
        self._temp_dir = tempfile.mkdtemp(prefix='mindsdb_fileds_')
        try:
            r = requests.get(self.file, stream=True)
            if r.status_code == 200:
                with open(os.path.join(self._temp_dir, 'file'), 'wb') as f:
                    for chunk in r:
                        f.write(chunk)
            else:
                raise Exception(f'Responce status code is {r.status_code}')
        except Exception as e:
            print(f'Error during getting {self.file}')
            print(e)
            raise

    def get_tables(self):
        """
        List all tabels in PostgreSQL without the system tables information_schema and pg_catalog
        """
        query = "SELECT * FROM information_schema.tables WHERE \
                 table_schema NOT IN ('information_schema', 'pg_catalog')"
        res = self.native_query(query)
        return res

    def describe_table(self, table_name):
        """
        List names and data types about the table coulmns
        """
        query = f"SELECT table_name, column_name, data_type FROM \
              information_schema.columns WHERE table_name='{table_name}';"
        result = self.native_query(query)
        return result

    # TODO: JOIN, SELECT INTO