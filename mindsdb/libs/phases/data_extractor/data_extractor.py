from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.helpers.text_helpers import hashtext
from mindsdb.external_libs.stats import calculate_sample_size

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
            df = df.sample(frac=1)

        return df


    def _get_prepared_input_df(self):
        """

        :return:
        """
        df = None

        # if transaction metadata comes with some data as from_data create the data frame
        if self.transaction.hmd['from_data'] is not None:
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
        g = df.columns.to_series().groupby(df.dtypes).groups

        if np.dtype('<M8[ns]') in g:
            for colname in g[np.dtype('<M8[ns]')]:
                df[colname] = df[colname].astype(str)
        return df


    def _validate_input_data_integrity(self):
        """

        :return:
        """



        if len(self.transaction.input_data.data_array) <= 0:
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
        result = self._get_prepared_input_df()

        columns = list(result.columns.values)
        data_array = list(result.values.tolist())

        self.transaction.input_data.columns = columns
        self.transaction.input_data.data_array = data_array

        self._validate_input_data_integrity()

        is_time_series = self.transaction.lmd['model_is_time_series']
        group_by = self.transaction.lmd['model_group_by']

        # create a list of the column numbers (indexes) that make the group by, this is so that we can greate group by hashes for each row
        if len(group_by)>0:
            group_by_col_indexes = [columns.index(group_by_column) for group_by_column in group_by]

        # create all indexes by group by, that is all the rows that belong to each group by
        self.transaction.input_data.all_indexes[KEY_NO_GROUP_BY] = []
        self.transaction.input_data.train_indexes[KEY_NO_GROUP_BY] = []
        self.transaction.input_data.test_indexes[KEY_NO_GROUP_BY] = []
        self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY] = []
        for i, row in enumerate(self.transaction.input_data.data_array):

            if len(group_by) > 0:
                group_by_value = '_'.join([str(row[group_by_index]) for group_by_index in group_by_col_indexes])

                if group_by_value not in self.transaction.input_data.all_indexes:
                    self.transaction.input_data.all_indexes[group_by_value] = []

                self.transaction.input_data.all_indexes[group_by_value] += [i]

            self.transaction.input_data.all_indexes[KEY_NO_GROUP_BY] += [i]

        # move indexes to corresponding train, test, validation, etc and trim input data accordingly
        for key in self.transaction.input_data.all_indexes:
            if len(self.transaction.input_data.all_indexes) > 1 and key == KEY_NO_GROUP_BY:
                continue

            length = len(self.transaction.input_data.all_indexes[key])
            if self.transaction.lmd['type'] == TRANSACTION_LEARN:
                sample_size = int(calculate_sample_size(population_size=length,
                                                        margin_error=self.transaction.lmd['sample_margin_of_error'],
                                                        confidence_level=self.transaction.lmd['sample_confidence_level']))

                # this evals True if it should send the entire group data into test, train or validation as opposed to breaking the group into the subsets
                should_split_by_group = type(group_by) == list and len(group_by) > 0

                if should_split_by_group:
                    self.transaction.input_data.train_indexes[key] = self.transaction.input_data.all_indexes[key][0:round(length - length*CONFIG.TEST_TRAIN_RATIO)]
                    self.transaction.input_data.train_indexes[KEY_NO_GROUP_BY].extend(self.transaction.input_data.train_indexes[key])

                    self.transaction.input_data.test_indexes[key] = self.transaction.input_data.all_indexes[key][round(length - length*CONFIG.TEST_TRAIN_RATIO):int(round(length - length*CONFIG.TEST_TRAIN_RATIO) + round(length*CONFIG.TEST_TRAIN_RATIO/2))]
                    self.transaction.input_data.test_indexes[KEY_NO_GROUP_BY].extend(self.transaction.input_data.test_indexes[key])

                    self.transaction.input_data.validation_indexes[key] = self.transaction.input_data.all_indexes[key][(round(length - length*CONFIG.TEST_TRAIN_RATIO) + round(length*CONFIG.TEST_TRAIN_RATIO/2)):]
                    self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY].extend(self.transaction.input_data.validation_indexes[key])

                else:
                    # make sure that the last in the time series are also the subset used for test
                    train_window = (0,int(length*(1-2*CONFIG.TEST_TRAIN_RATIO)))
                    self.transaction.input_data.train_indexes[key] = self.transaction.input_data.all_indexes[key][train_window[0]:train_window[1]]
                    validation_window = (train_window[1],train_window[1] + int(length*CONFIG.TEST_TRAIN_RATIO))
                    test_window = (validation_window[1],length)
                    self.transaction.input_data.test_indexes[key] = self.transaction.input_data.all_indexes[key][test_window[0]:test_window[1]]
                    self.transaction.input_data.validation_indexes[key] = self.transaction.input_data.all_indexes[key][validation_window[0]:validation_window[1]]

        # log some stats
        if self.transaction.lmd['type'] == TRANSACTION_LEARN:

            total_rows_used_by_subset = {'train': 0, 'test': 0, 'validation': 0}
            average_number_of_rows_used_per_groupby = {'train': 0, 'test': 0, 'validation': 0}
            number_of_groups_per_subset = {'train': 0, 'test': 0, 'validation': 0}

            for group_key in total_rows_used_by_subset:
                pointer = getattr(self.transaction.input_data, group_key+'_indexes')
                total_rows_used_by_subset[group_key] = sum([len(pointer[key_i]) for key_i in pointer])
                number_of_groups_per_subset[group_key] = len(pointer)
                #average_number_of_rows_used_per_groupby[group_key] = total_rows_used_by_subset[group_key] / number_of_groups_per_subset[group_key]


            total_rows_used = sum(total_rows_used_by_subset.values())
            total_rows_in_input = len(self.transaction.input_data.data_array)
            total_number_of_groupby_groups = len(self.transaction.input_data.all_indexes)

            if total_rows_used != total_rows_in_input:
                self.log.info('You requested to sample with a *margin of error* of {sample_margin_of_error} and a *confidence level* of {sample_confidence_level}. Therefore:'.format(sample_confidence_level=self.transaction.lmd['sample_confidence_level'], sample_margin_of_error= self.transaction.lmd['sample_margin_of_error']))
                self.log.info('Using a [Cochran\'s sample size calculator](https://www.statisticshowto.datasciencecentral.com/probability-and-statistics/find-sample-size/) we got the following sample sizes:')
                data = {
                    'total': [total_rows_in_input, 'Total number of rows in input'],
                    'subsets': [[total_rows_used, 'Total number of rows used']],
                    'label': 'Sample size for margin of error of ({sample_margin_of_error}) and a confidence level of ({sample_confidence_level})'.format(sample_confidence_level=self.transaction.lmd['sample_confidence_level'], sample_margin_of_error= self.transaction.lmd['sample_margin_of_error'])
                }
                self.log.infoChart(data, type='pie')

            '''
            if total_number_of_groupby_groups > 1:
                self.log.info('You are grouping your data by [{group_by}], we found:'.format(group_by=', '.join(group_by)))
                data = {
                    'Total number of groupby groups': total_number_of_groupby_groups,
                    'Average number of rows per groupby group': int(sum(average_number_of_rows_used_per_groupby.values())/len(average_number_of_rows_used_per_groupby))
                }
                self.log.infoChart(data, type='list')
            '''

            self.log.info('We have split the input data into:')

            data = {
                'subsets': [
                    [total_rows_used_by_subset['train'], 'Train'],
                    [total_rows_used_by_subset['test'], 'Test'],
                    [total_rows_used_by_subset['validation'], 'Validation']
                ],
                'label': 'Number of rows per subset'
            }

            self.log.infoChart(data, type='pie')



def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    CONFIG.DEBUG_BREAK_POINT = PHASE_DATA_EXTRACTOR

    mdb = Predictor(name='home_rentals')


    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02
    )






# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
