import pandas
import logging
import csv
import mindsdb.config as CONFIG
import re
import urllib3
from io import BytesIO, StringIO
import csv
import codecs
import json
import traceback

from mindsdb.libs.data_types.data_source import DataSource
from pandas.io.json import json_normalize

class FileDS(DataSource):

    def clean(self, header):

        clean_header = []
        col_count={}

        replace_chars = """ ,./;'[]!@#$%^&*()+{-=+~`}\\|:"<>?"""

        for col in header:
            orig_col = col
            for char in replace_chars:
                col = col.replace(char,'_')
            col = re.sub('_+','_',col)
            if col[-1] == '_':
                col = col[:-1]
            col_count[col] = 1 if col not in col_count else col_count[col]+1
            if col_count[col] > 1:
                col = col+'_'+str(col_count[col])

            if orig_col != col:
                logging.warning('[Column renamed] {orig_col} to {col}'.format(orig_col=orig_col, col=col))

            self._col_map[orig_col] = col
            clean_header.append(col)

        if clean_header != header:
            string = """\n    {cols} \n""".format(cols=",\n    ".join(clean_header))
            logging.warning('The Columns have changed, here are the renamed columns: \n {string}'.format(string=string))


        return  clean_header

    def cleanRow(self, row):
        n_row = []
        for cell in row:
            if str(cell) in ['', ' ', '  ', 'NaN', 'nan', 'NA']:
                cell = None
            n_row.append(cell)

        return n_row

    def _getDataIo(self, file):
        """
        This gets a file either url or local file and defiens what the format is as well as dialect

        :param file: file path or url
        :return: data_io, format, dialect
        """

        ############
        # get file as io object
        ############

        data = BytesIO()

        # get data from either url or file load in memory
        if file[:5] == 'http:' or file[:6] == 'https:':
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            http = urllib3.PoolManager()
            r = http.request('GET', file, preload_content=False)

            data.write(r.read())
            data.seek(0)

        # else read file from local file system
        else:
            try:
                data = open(file, 'rb')
            except Exception as e:
                error = 'Could not load file, possible exception : {exception}'.format(exception = e)
                logging.error(error)
                raise ValueError(error)


        dialect = None

        ############
        # check for file type
        ############

        # try to guess if its an excel file
        xlsx_sig = b'\x50\x4B\x05\06'
        xlsx_sig2 = b'\x50\x4B\x03\x04'
        xls_sig = b'\x09\x08\x10\x00\x00\x06\x05\x00'

        # differnt whence, offset, size for different types
        excel_meta = [ ('xls', 0, 512, 8), ('xlsx', 2, -22, 4)]

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

            except:
                data.seek(0)

        # if not excel it can be a json file or a CSV, convert from binary to stringio

        byte_str = data.read()
        # Move it to StringIO
        try:
            data = StringIO(byte_str.decode('UTF-8'))
        except:
            logging.error(traceback.format_exc())
            logging.error('Could not load into string')

        # see if its JSON
        buffer = data.read(100)
        data.seek(0)
        text = buffer.strip()
        # analyze first n characters
        if len(text) > 0:
            text = text.strip()
            # it it looks like a json, then try to parse it
            if text != "" and ((text[0] == "{") or (text[0] == "[")):
                try:
                    json.loads(data.read())
                    data.seek(0)
                    return data, 'json', dialect
                except:
                    data.seek(0)
                    return data, None, dialect

        # lets try to figure out if its a csv
        try:
            data.seek(0)
            full = len(data.read())
            data.seek(0)
            bytes_to_read = int(full*0.3)
            dialect = csv.Sniffer().sniff(data.read(bytes_to_read))
            data.seek(0)
            # if csv dialect identified then return csv
            if dialect:
                return data, 'csv', dialect
            else:
                return data, None, dialect
        except:
            data.seek(0)
            logging.error('Could not detect format for this file')
            logging.error(traceback.format_exc())
            # No file type identified
            return data, None, dialect




    def _setup(self,file, clean_header = True, clean_rows = True, custom_parser = None):
        """
        Setup from file
        :param file: fielpath or url
        :param clean_header: if you want to clean header column names
        :param clean_rows:  if you want to clean rows for strange null values
        :param custom_parser: if you want to parse the file with some custom parser

        """

        # get file data io, format and dialect
        data, format, dialect = self._getDataIo(file)
        data.seek(0) # make sure we are at 0 in file pointer

        if format is None:
            logging.error('Could not laod file into any format, supported formats are csv, json, xls, xslx')

        if custom_parser:
            header, file_data = custom_parser(data, format)

        elif format == 'csv':

            csv_reader = list(csv.reader(data, dialect))
            header = csv_reader[0]
            file_data =  csv_reader[1:]


        elif format in ['xlsx', 'xls']:
            data.seek(0)
            df = pandas.read_excel(data)
            header = df.columns.values.tolist()
            file_data = df.values.tolist()

        elif format == 'json':
            data.seek(0)
            json_doc = json.loads(data.read())
            df = json_normalize(json_doc)
            header = df.columns.values.tolist()
            file_data = df.values.tolist()

        if clean_header == True:
            header = self.clean(header)

        if clean_rows == True:
            file_list_data = []
            for row in file_data:
                row = self.cleanRow(row)
                file_list_data.append(row)
        else:
            file_list_data = file_data

        self.setDF(pandas.DataFrame(file_list_data, columns=header))





