from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.mindsdb_logger import log

from mindsdb.libs.constants.mindsdb import *

class DataSplitter(BaseModule):
    def run(self):
        group_by = self.transaction.lmd['model_group_by']
        if group_by is None or len(group_by) == 0:
            group_by = []
            for col in self.transaction.lmd['predict_columns']:
                if self.transaction.lmd['column_stats'][col]['data_type'] == DATA_TYPES.CATEGORICAL:
                    group_by.append(col)
            if len(group_by) > 0:
                self.transaction.input_data.data_frame = self.transaction.input_data.data_frame.sort_values(group_by)

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
                group_by_value = '_'.join([str(row[group_by_index]) for group_by_index in [self.transaction.input_data.columns.index(group_by_col) for group_by_col in group_by]])

                if group_by_value not in all_indexes:
                    all_indexes[group_by_value] = []

                all_indexes[group_by_value] += [i]

            all_indexes[KEY_NO_GROUP_BY] += [i]

        # move indexes to corresponding train, test, validation, etc and trim input data accordingly
        if self.transaction.lmd['type'] == TRANSACTION_LEARN:
            for key in all_indexes:
                should_split_by_group = type(group_by) == list and len(group_by) > 0

                #If this is a group by, skip the `KEY_NO_GROUP_BY` key
                if should_split_by_group and key == KEY_NO_GROUP_BY:
                    continue

                length = len(all_indexes[key])
                # this evals True if it should send the entire group data into test, train or validation as opposed to breaking the group into the subsets
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
        '''
        print(len(self.transaction.input_data.train_df[self.transaction.input_data.train_df['default.payment.next.month'] == '1']))
        print(len(self.transaction.input_data.train_df[self.transaction.input_data.train_df['default.payment.next.month'] == '0']))
        print(len(self.transaction.input_data.test_df[self.transaction.input_data.test_df['default.payment.next.month'] == '1']))
        print(len(self.transaction.input_data.test_df[self.transaction.input_data.test_df['default.payment.next.month'] == '0']))
        print(len(self.transaction.input_data.validation_df[self.transaction.input_data.validation_df['default.payment.next.month'] == '1']))
        print(len(self.transaction.input_data.validation_df[self.transaction.input_data.validation_df['default.payment.next.month'] == '0']))
        exit()
        '''
        # --- Dataset split into train/test/validate --- #
