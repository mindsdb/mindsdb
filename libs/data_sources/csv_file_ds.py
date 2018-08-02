import pandas
import json

from libs.data_types.data_source import DataSource

class CSVFileDS(DataSource):

    def __init__(self, filepath):
        self._df = pandas.read_csv(filepath)

