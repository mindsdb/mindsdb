from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.helpers.text_helpers import hashtext
from mindsdb.external_libs.stats import calculate_sample_size
from pandas.api.types import is_numeric_dtype

import random
import traceback
import pandas
import numpy as np


class DataExtractor(BaseModule):

    phase_name = PHASE_DATA_EXTRACTOR

    def _get_data_frame_from_when_conditions(self):
        """
        :return:
        """

        columns = self.transaction.lmd['columns']
        when_conditions = self.transaction.hmd['model_when_conditions']

        when_conditions_list = []
        # here we want to make a list of the type  ( ValueForField1, ValueForField2,..., ValueForFieldN ), ...
        for when_condition in when_conditions:
            cond_list = [None] * len(columns)  # empty list with blanks for values

            for condition_col in when_condition:
                col_index = columns.index(condition_col)
                cond_list[col_index] = when_condition[condition_col]

            when_conditions_list.append(cond_list)

        result = pandas.DataFrame(when_conditions_list, columns=columns)

        return result


    def _apply_sort_conditions_to_df(self, df):
        """

        :param df:
        :return:
        """

        # apply order by (group_by, order_by)
        if self.transaction.lmd['model_is_time_series']:
            asc_values = [order_tuple[ORDER_BY_KEYS.ASCENDING_VALUE] for order_tuple in self.transaction.lmd['model_order_by']]
            sort_by = [order_tuple[ORDER_BY_KEYS.COLUMN] for order_tuple in self.transaction.lmd['model_order_by']]

            if self.transaction.lmd['model_group_by']:
                sort_by = self.transaction.lmd['model_group_by'] + sort_by
                asc_values = [True for i in self.transaction.lmd['model_group_by']] + asc_values
            df = df.sort_values(sort_by, ascending=asc_values)

        elif self.transaction.lmd['type'] == TRANSACTION_LEARN:
            # if its not a time series, randomize the input data and we are learning
            df = df.sample(frac=1, random_state=len(df))

        return df


    def _get_prepared_input_df(self):
        """

        :return:
        """
        df = None

        # if transaction metadata comes with some data as from_data create the data frame
        if 'from_data' in self.transaction.hmd and self.transaction.hmd['from_data'] is not None:
            # make sure we build a dataframe that has all the columns we need
            df = self.transaction.hmd['from_data']
            df = df.where((pandas.notnull(df)), None)

        # if this is a predict statement, create use model_when_conditions to shape the dataframe
        if  self.transaction.lmd['type'] == TRANSACTION_PREDICT:
            if self.transaction.hmd['when_data'] is not None:
                df = self.transaction.hmd['when_data']
                df = df.where((pandas.notnull(df)), None)

            elif self.transaction.hmd['model_when_conditions'] is not None:

                # if no data frame yet, make one
                df = self._get_data_frame_from_when_conditions()


        # if by now there is no DF, throw an error
        if df is None:
            error = 'Could not create a data frame for transaction'
            self.log.error(error)
            raise ValueError(error)
            return None

        df = self._apply_sort_conditions_to_df(df)
        groups = df.columns.to_series().groupby(df.dtypes).groups

        boolean_dictionary = {True: 'True', False: 'False'}
        numeric_dictionary = {True: 1, False: 0}
        for column in df:
            if is_numeric_dtype(df[column]):
                df[column] = df[column].replace(numeric_dictionary)
            else:
                df[column] = df[column].replace(boolean_dictionary)

        if np.dtype('datetime64[ns]') in groups:
            for colname in groups[np.dtype('datetime64[ns]')]:
                df[colname] = df[colname].astype(str)

        return df


    def _validate_input_data_integrity(self):
        """
        :return:
        """
        if self.transaction.input_data.data_frame.shape[0] <= 0:
            error = 'Input Data has no rows, please verify from_data or when_conditions'
            self.log.error(error)
            raise ValueError(error)

        # make sure that the column we are trying to predict is on the input_data
        # else fail, because we cannot predict data we dont have

        #if self.transaction.lmd['model_is_time_series'] or self.transaction.lmd['type'] == TRANSACTION_LEARN:
        # ^ How did this even make sense before ? Why did it not crash tests ? Pressumably because the predict col was loaded into `input_data` as an empty col

        if self.transaction.lmd['type'] == TRANSACTION_LEARN:
            for col_target in self.transaction.lmd['predict_columns']:
                if col_target not in self.transaction.input_data.columns:
                    err = 'Trying to predict column {column} but column not in source data'.format(column=col_target)
                    self.log.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    raise ValueError(err)
                    return


    def run(self):
        # --- Dataset gets randomized or sorted (if timeseries) --- #
        result = self._get_prepared_input_df()
        # --- Dataset gets randomized or sorted (if timeseries) --- #

        # --- Some information about the dataset gets transplanted into transaction level variables --- #
        self.transaction.input_data.columns = result.columns.values.tolist()
        self.transaction.lmd['columns'] = self.transaction.input_data.columns
        self.transaction.input_data.data_frame = result
        # --- Some information about the dataset gets transplanted into transaction level variables --- #

        # --- Some preliminary dataset integrity checks --- #
        self._validate_input_data_integrity()
        # --- Some preliminary dataset integrity checks --- #


        # --- Dataset split into train/test/validate --- #
        is_time_series = self.transaction.lmd['model_is_time_series']
        group_by = self.transaction.lmd['model_group_by']
        KEY_NO_GROUP_BY = '{PLEASE_DONT_TELL_ME_ANYONE_WOULD_CALL_A_COLUMN_THIS}##ALL_ROWS_NO_GROUP_BY##{PLEASE_DONT_TELL_ME_ANYONE_WOULD_CALL_A_COLUMN_THIS}'

        # create all indexes by group by, that is all the rows that belong to each group by
        all_indexes = {}
        train_indexes = {}
        test_indexes = {}
        validation_indexes = {}

        all_indexes[KEY_NO_GROUP_BY] = []
        train_indexes[KEY_NO_GROUP_BY] = []
        test_indexes[KEY_NO_GROUP_BY] = []
        validation_indexes[KEY_NO_GROUP_BY] = []
        for i, row in self.transaction.input_data.data_frame.iterrows():

            if len(group_by) > 0:
                group_by_value = '_'.join([str(row[group_by_index]) for group_by_index in [columns.index(group_by_column) for group_by_column in group_by]])

                if group_by_value not in all_indexes:
                    all_indexes[group_by_value] = []

                all_indexes[group_by_value] += [i]

            all_indexes[KEY_NO_GROUP_BY] += [i]

        # move indexes to corresponding train, test, validation, etc and trim input data accordingly
        for key in all_indexes:
            #If this is a group by, skip the `KEY_NO_GROUP_BY` key
            if len(all_indexes) > 1 and key == KEY_NO_GROUP_BY:
                continue

            length = len(all_indexes[key])
            if self.transaction.lmd['type'] == TRANSACTION_LEARN:
                # this evals True if it should send the entire group data into test, train or validation as opposed to breaking the group into the subsets
                should_split_by_group = type(group_by) == list and len(group_by) > 0

                if should_split_by_group:
                    train_indexes[key] = all_indexes[key][0:round(length - length*CONFIG.TEST_TRAIN_RATIO)]
                    train_indexes[KEY_NO_GROUP_BY].extend(train_indexes[key])

                    test_indexes[key] = all_indexes[key][round(length - length*CONFIG.TEST_TRAIN_RATIO):int(round(length - length*CONFIG.TEST_TRAIN_RATIO) + round(length*CONFIG.TEST_TRAIN_RATIO/2))]
                    test_indexes[KEY_NO_GROUP_BY].extend(test_indexes[key])

                    validation_indexes[key] = all_indexes[key][(round(length - length*CONFIG.TEST_TRAIN_RATIO) + round(length*CONFIG.TEST_TRAIN_RATIO/2)):]
                    validation_indexes[KEY_NO_GROUP_BY].extend(validation_indexes[key])

                else:
                    # make sure that the last in the time series are also the subset used for test
                    train_window = (0,int(length*(1-2*CONFIG.TEST_TRAIN_RATIO)))
                    train_indexes[key] = all_indexes[key][train_window[0]:train_window[1]]
                    validation_window = (train_window[1],train_window[1] + int(length*CONFIG.TEST_TRAIN_RATIO))
                    test_window = (validation_window[1],length)
                    test_indexes[key] = all_indexes[key][test_window[0]:test_window[1]]
                    validation_indexes[key] = all_indexes[key][validation_window[0]:validation_window[1]]

        self.transaction.input_data.train_df = self.transaction.input_data.data_frame.iloc[train_indexes[KEY_NO_GROUP_BY]].copy()
        self.transaction.input_data.test_df = self.transaction.input_data.data_frame.iloc[test_indexes[KEY_NO_GROUP_BY]].copy()
        self.transaction.input_data.validation_df = self.transaction.input_data.data_frame.iloc[validation_indexes[KEY_NO_GROUP_BY]].copy()

        self.transaction.lmd['data_preparation']['test_row_count'] = len(self.transaction.input_data.test_df)
        self.transaction.lmd['data_preparation']['train_row_count'] = len(self.transaction.input_data.train_df)
        self.transaction.lmd['data_preparation']['validation_row_count'] = len(self.transaction.input_data.validation_df)

        # @TODO: Consider deleting self.transaction.input_data.data_frame here

        # log some stats
        if self.transaction.lmd['type'] == TRANSACTION_LEARN:
            # @TODO I don't think the above works, fix at some point or just remove `sample_margin_of_error` option from the interface
            if len(self.transaction.input_data.data_frame) != sum([len(self.transaction.input_data.train_df),len(self.transaction.input_data.test_df),len(self.transaction.input_data.validation_df)]):
                self.log.info('You requested to sample with a *margin of error* of {sample_margin_of_error} and a *confidence level* of {sample_confidence_level}. Therefore:'.format(sample_confidence_level=self.transaction.lmd['sample_confidence_level'], sample_margin_of_error= self.transaction.lmd['sample_margin_of_error']))
                self.log.info('Using a [Cochran\'s sample size calculator](https://www.statisticshowto.datasciencecentral.com/probability-and-statistics/find-sample-size/) we got the following sample sizes:')
                data = {
                    'total': [total_rows_in_input, 'Total number of rows in input'],
                    'subsets': [[total_rows_used, 'Total number of rows used']],
                    'label': 'Sample size for margin of error of ({sample_margin_of_error}) and a confidence level of ({sample_confidence_level})'.format(sample_confidence_level=self.transaction.lmd['sample_confidence_level'], sample_margin_of_error= self.transaction.lmd['sample_margin_of_error'])
                }
                self.log.infoChart(data, type='pie')
            # @TODO Bad code ends here (see @TODO above)

            data = {
                'subsets': [
                    [len(self.transaction.input_data.train_df), 'Train'],
                    [len(self.transaction.input_data.test_df), 'Test'],
                    [len(self.transaction.input_data.validation_df), 'Validation']
                ],
                'label': 'Number of rows per subset'
            }

            self.log.info('We have split the input data into:')
            self.log.infoChart(data, type='pie')
    # --- Dataset split into train/test/validate --- #
