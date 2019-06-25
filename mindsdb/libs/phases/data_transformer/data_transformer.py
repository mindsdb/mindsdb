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

        # Un-bias dataset for training
        for colum in self.transaction.lmd['predict_columns']:
            if self.transaction.lmd['column_stats'][column]['data_subtype'] == DATA_SUBTYPES.SINGLE:
                occurance_map = {}
                ciclying_map = {}

                for i in range(0,len(self.transaction.lmd['column_stats'][column]['histogram']['x'])):
                    ciclying_map[self.transaction.lmd['column_stats'][column]['histogram']['x'][i]] = 0
                    occurance_map[self.transaction.lmd['column_stats'][column]['histogram']['x'][i]] = self.transaction.lmd['column_stats'][column]['histogram']['y'][i]

                print(occurance_map)

                max_val_occurances = max(occurance_map.values())
                for val in occurance_map:
                    while occurance_map[val] < max_val_occurances:
                        print(ciclying_map[val])
                        copied_row = input_data.data_frame[input_data.data_frame[colum]].reindex().iloc[ciclying_map[val]]

                        input_data.data_frame[column].append(copied_row)
                        input_data.train_df[column].append(copied_row)

                        index = len(input_data.data_frame)
                        self.transaction.input_data.all_indexes[KEY_NO_GROUP_BY].append(index)
                        self.transaction.input_data.train_indexes[KEY_NO_GROUP_BY].append(index)

                        occurance_map[val] += 1
                        ciclying_map[val] += 1
