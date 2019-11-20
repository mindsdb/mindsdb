from dateutil.parser import parse as parse_datetime
import datetime
import math
import sys

import pandas as pd
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import clean_float
from mindsdb.libs.helpers.debugging import *


class DataTransformer(BaseModule):

    @staticmethod
    def _handle_nan(x):
        if x is not None and math.isnan(x):
            return 0
        else:
            return x

    @staticmethod
    def _try_round(x):
        try:
            return round(x)
        except:
            return None

    @staticmethod
    def _standardize_date(date_str):
        try:
            # will return a datetime object
            date = parse_datetime(date_str)
        except:
            try:
                date = datetime.datetime.utcfromtimestamp(date_str)
            except:
                return None
        # Uncomment if we want to work internally date type
        #return date.date()

        # Uncomment if we want to work internally with string type
        return date.strftime('%Y-%m-%d')

    @staticmethod
    def _standardize_datetime(date_str):
        try:
            # will return a datetime object
            dt = parse_datetime(str(date_str))
        except:
            try:
                dt = datetime.datetime.utcfromtimestamp(date_str)
            except:
                return None
        # Uncomment if we want to work internally date type
        #return dt

        # Uncomment if we want to work internally with string type
        # @TODO Decide if we ever need/want the milliseconds
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def _lightwood_datetime_processing(dt):
        dt = pd.to_datetime(dt, errors = 'coerce')
        try:
            return dt.timestamp()
        except:
            # @TODO Return `None` after appropriate changes in lightwood
            return None

    @staticmethod
    def _aply_to_all_data(input_data, column, func, transaction_type):
        input_data.data_frame[column] = input_data.data_frame[column].apply(func)
        if transaction_type == TRANSACTION_LEARN:
            input_data.train_df[column] = input_data.train_df[column].apply(func)
            input_data.test_df[column] = input_data.test_df[column].apply(func)
            input_data.validation_df[column] = input_data.validation_df[column].apply(func)

    @staticmethod
    def _cast_all_data(input_data, column, cast_to_type, transaction_type):
        input_data.data_frame[column] = input_data.data_frame[column].astype(cast_to_type)
        if transaction_type == TRANSACTION_LEARN:
            input_data.train_df[column] = input_data.train_df[column].astype(cast_to_type)
            input_data.test_df[column] = input_data.test_df[column].astype(cast_to_type)
            input_data.validation_df[column] = input_data.validation_df[column].astype(cast_to_type)


    from memory_profiler import profile
    #@profile
    def run(self, input_data):
        print_mem()
        for column in input_data.columns:

            if column in self.transaction.lmd['malformed_columns']:
                continue

            data_type = self.transaction.lmd['column_stats'][column]['data_type']
            data_subtype = self.transaction.lmd['column_stats'][column]['data_subtype']

            if data_type == DATA_TYPES.NUMERIC:
                self._aply_to_all_data(input_data, column, clean_float, self.transaction.lmd['type'])
                self._aply_to_all_data(input_data, column, self._handle_nan, self.transaction.lmd['type'])

                if data_subtype == DATA_SUBTYPES.INT:
                    self._aply_to_all_data(input_data, column, DataTransformer._try_round, self.transaction.lmd['type'])

            if data_type == DATA_TYPES.DATE:
                if data_subtype == DATA_SUBTYPES.DATE:
                    self._aply_to_all_data(input_data, column, self._standardize_date, self.transaction.lmd['type'])

                elif data_subtype == DATA_SUBTYPES.TIMESTAMP:
                    self._aply_to_all_data(input_data, column, self._standardize_datetime, self.transaction.lmd['type'])

            if data_type == DATA_TYPES.CATEGORICAL:
                    self._cast_all_data(input_data, column, 'category', self.transaction.lmd['type'])

            if self.transaction.hmd['model_backend'] == 'lightwood':
                if data_type == DATA_TYPES.DATE:
                    self._aply_to_all_data(input_data, column, self._standardize_datetime, self.transaction.lmd['type'])
                    self._aply_to_all_data(input_data, column, self._lightwood_datetime_processing, self.transaction.lmd['type'])
                    self._aply_to_all_data(input_data, column, self._handle_nan, self.transaction.lmd['type'])

        # Initialize this here, will be overwritten if `equal_accuracy_for_all_output_categories` is specified to be True in order to account for it
        self.transaction.lmd['weight_map'] = self.transaction.lmd['output_categories_importance_dictionary']

        # Un-bias dataset for training
        for column in self.transaction.lmd['predict_columns']:
            if self.transaction.lmd['column_stats'][column]['data_type'] == DATA_TYPES.CATEGORICAL and self.transaction.lmd['equal_accuracy_for_all_output_categories'] == True and self.transaction.lmd['type'] == TRANSACTION_LEARN:
                occurance_map = {}
                ciclying_map = {}

                for i in range(0,len(self.transaction.lmd['column_stats'][column]['histogram']['x'])):
                    ciclying_map[self.transaction.lmd['column_stats'][column]['histogram']['x'][i]] = 0
                    occurance_map[self.transaction.lmd['column_stats'][column]['histogram']['x'][i]] = self.transaction.lmd['column_stats'][column]['histogram']['y'][i]

                max_val_occurances = max(occurance_map.values())

                if self.transaction.hmd['model_backend'] in ('lightwood'):
                    lightwood_weight_map = {}
                    for val in occurance_map:
                        lightwood_weight_map[val] = 1/occurance_map[val] #sum(occurance_map.values())

                        if column in self.transaction.lmd['output_categories_importance_dictionary']:
                            if val in self.transaction.lmd['output_categories_importance_dictionary'][column]:
                                lightwood_weight_map[val] = lightwood_weight_map[val] * self.transaction.lmd['output_categories_importance_dictionary'][column][val]
                            elif '<default>' in self.transaction.lmd['output_categories_importance_dictionary'][column]:
                                lightwood_weight_map[val] = lightwood_weight_map[val] * self.transaction.lmd['output_categories_importance_dictionary'][column]['<default>']

                    self.transaction.lmd['weight_map'][column] = lightwood_weight_map

                column_is_weighted_in_train = column in self.transaction.lmd['weight_map']

                for val in occurance_map:
                    copied_rows_train = []
                    copied_rows_test = []
                    copied_rows_validate = []

                    data_frame_length = len(input_data.data_frame)
                    valid_rows = input_data.data_frame[input_data.data_frame[column] == val]

                    while occurance_map[val] < max_val_occurances:
                        if ciclying_map[val] >= len(valid_rows.index) - 1:
                            if len(copied_rows_train) > 0:
                                input_data.data_frame = input_data.data_frame.append(copied_rows_train)
                                input_data.train_df = input_data.train_df.append(copied_rows_train)

                            if len(copied_rows_test) > 0:
                                input_data.data_frame = input_data.data_frame.append(copied_rows_test)
                                input_data.test_df = input_data.test_df.append(copied_rows_test)

                            if len(copied_rows_validate) > 0:
                                input_data.data_frame = input_data.data_frame.append(copied_rows_validate)
                                input_data.validation_df = input_data.validation_df.append(copied_rows_validate)

                            copied_rows_train = []
                            copied_rows_test = []
                            copied_rows_validate = []

                            ciclying_map[val] = 0

                        index = list(valid_rows.index)[ciclying_map[val]]

                        if index in input_data.train_df.index and not column_is_weighted_in_train:
                            data_frame_length = data_frame_length + 1
                            copied_row = valid_rows.iloc[ciclying_map[val]]
                            copied_rows_train.append(copied_row)

                        elif index in input_data.test_df.index and not column_is_weighted_in_train:
                            data_frame_length = data_frame_length + 1
                            copied_row = valid_rows.iloc[ciclying_map[val]]

                            copied_rows_test.append(copied_row)

                        elif index in input_data.validation_df.index:
                            data_frame_length = data_frame_length + 1
                            copied_row = valid_rows.iloc[ciclying_map[val]]
                            copied_rows_validate.append(copied_row)

                        occurance_map[val] += 1
                        ciclying_map[val] += 1

                    if len(copied_rows_train) > 0:
                        input_data.data_frame = input_data.data_frame.append(copied_rows_train)
                        input_data.train_df = input_data.train_df.append(copied_rows_train)

                    if len(copied_rows_test) > 0:
                        input_data.data_frame = input_data.data_frame.append(copied_rows_test)
                        input_data.test_df = input_data.test_df.append(copied_rows_test)

                    if len(copied_rows_validate) > 0:
                        input_data.data_frame = input_data.data_frame.append(copied_rows_validate)
                        input_data.validation_df = input_data.validation_df.append(copied_rows_validate)
