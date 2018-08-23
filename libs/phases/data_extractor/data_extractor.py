"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

import config as CONFIG
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule

from collections import OrderedDict

import sys
import json
import random
import traceback
import sqlite3
import pandas
import json


class DataExtractor(BaseModule):

    phase_name = PHASE_DATA_EXTRACTION

    def run(self):

        if self.transaction.metadata.model_order_by:
            order_by_fields = self.transaction.metadata.model_order_by if self.transaction.metadata.model_group_by is None else [self.transaction.metadata.model_group_by] + self.transaction.metadata.model_order_by
        else:
            order_by_fields = []

        order_by_string = ", ".join(order_by_fields)

        if len(order_by_fields):
            query_wrapper = '''select * from ({orig_query}) orgi order by {order_by_string}'''
        else:
            query_wrapper = '''{orig_query}'''

        try:
            query = query_wrapper.format(orig_query = self.transaction.metadata.model_query, order_by_string=order_by_string)

            self.transaction.session.logging.info('About to pull query {query}'.format(query=query))

            #drill = self.session.drill.query(self.transaction.input_data_sql, timeout=CONFIG.DRILL_TIMEOUT)

            conn = sqlite3.connect("/tmp/mindsdb")
            print(self.transaction.metadata.model_query)
            df = pandas.read_sql_query(query, conn)

            result = df.where((pandas.notnull(df)), None)

            df = None


        except Exception:

            # If testing offline, get results from a .cache file
            self.session.logging.error(traceback.print_exc())
            self.transaction.error =True
            self.transaction.errorMsg = traceback.print_exc(1)
            return



        columns = list(result.columns.values)
        data_array = list(result.values.tolist())

        self.transaction.input_data.columns = columns


        if len(data_array[0])>0 and  self.transaction.metadata.model_predict_columns:
            for col_target in self.transaction.metadata.model_predict_columns:
                if col_target not in self.transaction.input_data.columns:
                    err = 'Trying to predict column {column} but column not in source data'.format(column=col_target)
                    self.session.logging.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    return

        self.transaction.input_data.data_array = data_array

        # extract test data if this is a learn transaction and there is a test query
        if self.transaction.metadata.type == TRANSACTION_LEARN:

            if self.transaction.metadata.model_test_query:
                try:
                    test_query = query_wrapper.format(orig_query = self.transaction.metadata.test_query, order_by_string= order_by_string)
                    self.transaction.session.logging.info('About to pull TEST query {query}'.format(query=test_query))
                    #drill = self.session.drill.query(test_query, timeout=CONFIG.DRILL_TIMEOUT)
                    df = pandas.read_sql_query(test_query, conn)
                    result = df.where((pandas.notnull(df)), None)
                    df = None

                    #result = vars(drill)['data']
                except Exception:

                    # If testing offline, get results from a .cache file
                    self.session.logging.error(traceback.print_exc())
                    self.transaction.error = True
                    self.transaction.errorMsg = traceback.print_exc(1)
                    return

                columns = list(result.columns.values)
                data_array = result.values.tolist()

                # Make sure that test adn train sets match column wise
                if columns != self.transaction.input_data.columns:
                    err = 'Trying to get data for test but columns in train set and test set dont match'
                    self.session.logging.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    return
                total_data_array = len(self.transaction.input_data.data_array)
                total_test_array =  len(data_array)
                test_indexes = [i for i in range(total_data_array, total_data_array+total_test_array)]

                self.transaction.input_data.test_data_indexes = test_indexes
                # make the input data relevant
                self.transaction.input_data.data_array += data_array

                # we later use this to either regenerate or not
                test_prob = 0

            else:
                test_prob = CONFIG.TEST_TRAIN_RATIO

            validation_prob = CONFIG.TEST_TRAIN_RATIO / (1-test_prob)

            group_by = self.transaction.metadata.model_group_by

            if group_by:
                try:
                    group_by_index = self.transaction.input_data.columns.index(group_by)
                except:
                    group_by_index = None
                    err = 'Trying to group by, {column} but column not in source data'.format(column=group_by)
                    self.session.logging.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    return

                # get unique group by values
                all_group_by_items_query = ''' select distinct {group_by_column} as grp from ( {query} ) sub'''.format(group_by_column=group_by, query=self.transaction.metadata.model_query)
                self.transaction.session.logging.info('About to pull GROUP BY query {query}'.format(query=all_group_by_items_query))
                df = pandas.read_sql_query(all_group_by_items_query, conn)
                result = df.where((pandas.notnull(df)), None)
                # create a list of values in group by, this is because result is array of array we want just array
                all_group_by_values = [i[0] for i in result.values.tolist()]

                # we will fill these depending on the test_prob and validation_prob
                test_group_by_values = []
                validation_group_by_values = []
                train_group_by_values = []

                # split the data into test, validation, train by group by data
                for group_by_value in all_group_by_values:

                    # depending on a random number if less than x_prob belongs to such group
                    # remember that test_prob can be 0 or the config value depending on if the test test was passed as a query
                    if float(random.random()) < test_prob:
                        test_group_by_values += [group_by_value]
                    elif float(random.random()) < validation_prob:
                        validation_group_by_values += [group_by_value]
                    else:
                        train_group_by_values += [group_by_value]

            for i, row in enumerate(self.transaction.input_data.data_array):

                in_test = True if i in self.transaction.input_data.test_indexes else False
                if not in_test:
                    if group_by:

                        group_by_value = row[group_by_index]
                        if group_by_value in test_group_by_values or len(self.transaction.input_data.test_indexes) == 0:
                            self.transaction.input_data.test_indexes += [i]
                        elif group_by_value in train_group_by_values or len(self.transaction.input_data.train_indexes) == 0:
                            self.transaction.input_data.train_indexes += [i]
                        elif group_by_value in validation_group_by_values or len(self.transaction.input_data.validation_indexes) == 0:
                            self.transaction.input_data.validation_indexes += [i]

                    else:
                        # remember that test_prob can be 0 or the config value depending on if the test test was passed as a query
                        if float(random.random()) <= test_prob or len(self.transaction.input_data.test_indexes) == 0:
                            self.transaction.input_data.test_indexes += [i]
                        elif float(random.random()) <= validation_prob or len(self.transaction.input_data.validation_indexes)==0:
                            self.transaction.input_data.validation_indexes += [i]
                        else:
                            self.transaction.input_data.train_indexes += [i]










def test():
    from libs.controllers.mindsdb_controller import MindsDBController as MindsDB

    mdb = MindsDB()
    mdb.learn(from_query='select * from position_tgt', group_by = 'id', order_by=['max_time_rec'], predict='position', model_name='mdsb_model', test_query=None, breakpoint = PHASE_DATA_EXTRACTION)

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

