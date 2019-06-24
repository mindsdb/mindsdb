from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import clean_float


class DataTransformer(BaseModule):

    @staticmethod
    def _try_round(x):
        try:
            return round(x)
        except:
            return None

    def run(self, input_data):
        for column in input_data.columns:
            data_type = self.transaction.lmd['column_stats'][column]['data_type']
            data_stype = self.transaction.lmd['column_stats'][column]['data_subtype']

            if data_type == DATA_TYPES.NUMERIC:
                input_data.data_frame[column] = input_data.data_frame[column].apply(clean_float)
                input_data.train_df[column] = input_data.train_df[column].apply(clean_float)
                input_data.test_df[column] = input_data.test_df[column].apply(clean_float)
                input_data.validation_df[column] = input_data.validation_df[column].apply(clean_float)

                if data_stype == DATA_SUBTYPES.INT:
                    input_data.data_frame[column] = input_data.data_frame[column].apply(DataTransformer._try_round)
                    input_data.train_df[column] = input_data.train_df[column].apply(DataTransformer._try_round)
                    input_data.test_df[column] = input_data.test_df[column].apply(DataTransformer._try_round)
                    input_data.validation_df[column] = input_data.validation_df[column].apply(DataTransformer._try_round)
