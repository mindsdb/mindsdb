import pandas
import logging
import csv
import mindsdb.config as CONFIG
import re

from mindsdb.libs.data_types.data_source import DataSource

class CSVFileDS(DataSource):

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
                logging.warn('[Column renamed] {orig_col} to {col}'.format(orig_col=orig_col, col=col))
            clean_header.append(col)

        return  clean_header

    def cleanRow(self, row):
        n_row = []
        for cell in row:
            if str(cell) in ['', ' ', '  ', 'NaN', 'nan', 'NA']:
                cell = None
            n_row.append(cell)

        return n_row


    def __init__(self, filepath, clean_header = True, clean_rows = True):

        if clean_header == False:
            self._df = pandas.read_csv(filepath)
        else:
            with open(filepath) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                file_list_data = []
                header = []
                for row in csv_reader:
                    # is this a header
                    if len(header) == 0:
                        header = self.clean(row)
                    else:
                        if clean_rows == True:
                            row = self.cleanRow(row)
                        file_list_data.append(row)


                self.setDF(pandas.DataFrame(file_list_data, columns=header))



#CSVFileDS(CONFIG.MINDSDB_STORAGE_PATH+'/vavo.csv')