from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import clean_float


class DataTransformer(BaseModule):

    def run(self, input_data):
        for column in input_data.columns:
            data_type = self.transaction.lmd['column_stats'][column]['data_type']
            data_stype = self.transaction.lmd['column_stats'][column]['data_subtype']

            if data_type == DATA_TYPES.NUMERIC:
                input_data.data_frame[column] =

            self.transaction.input_data
