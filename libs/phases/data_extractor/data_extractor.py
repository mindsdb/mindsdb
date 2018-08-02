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
        try:
            if self.transaction.predict_metadata is None:
                query = self.transaction.input_data_sql
            else:
                query = self.transaction.predict_metadata[KEY_MODEL_QUERY]
            self.transaction.session.logging.info('About to pull query {query}'.format(query=self.transaction.input_data_sql))

            #drill = self.session.drill.query(self.transaction.input_data_sql, timeout=CONFIG.DRILL_TIMEOUT)

            conn = sqlite3.connect("/tmp/mindsdb")
            print(self.transaction.input_data_sql)
            df = pandas.read_sql_query(self.transaction.input_data_sql, conn)

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

        # TODO: ABstract this into the drill driver
        # data_array = []
        # for row in data:
        #     row_array = []
        #     for column in columns:
        #         row_array += [row[column]]
        #     data_array += [row_array]

        self.transaction.input_metadata = {
            KEY_COLUMNS: columns
        }

        if len(data_array[0])>0 and self.transaction.model_metadata and self.transaction.model_metadata[KEY_MODEL_PREDICT_COLUMNS]:
            for col_target in self.transaction.model_metadata[KEY_MODEL_PREDICT_COLUMNS]:
                if col_target not in self.transaction.input_metadata[KEY_COLUMNS]:
                    err = 'Trying to predict column {column} but column not in source data'.format(column=col_target)
                    self.session.logging.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    return

        self.transaction.input_data_array = data_array

        # extract test data if this is a learn transaction and there is a test query
        if self.transaction.type == TRANSACTION_LEARN \
                and KEY_MODEL_TEST_QUERY in self.transaction.model_metadata \
                and self.transaction.model_metadata[KEY_MODEL_TEST_QUERY]:
            try:
                test_query = self.transaction.model_metadata[KEY_MODEL_TEST_QUERY]
                self.transaction.session.logging.info(
                    'About to pull TEST query {query}'.format(query=self.transaction.input_data_sql))
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
            if columns != self.transaction.input_metadata[KEY_COLUMNS]:
                err = 'Trying to get data for test but columns in train set and test set dont match'.format(column=col_target)
                self.session.logging.error(err)
                self.transaction.error = True
                self.transaction.errorMsg = err
                return

            # # TODO: ABstract this into the drill driver
            # data_array = []
            # for row in data:
            #     row_array = []
            #     for column in columns:
            #         row_array += [row[column]]
            #     data_array += [row_array]




            self.transaction.input_test_data_array = data_array

def test():

    from libs.test.test_controller import TestController

    module = TestController('CREATE MODEL FROM (SELECT * FROM vitals_tgt) AS vitals PREDICT vitals ', PHASE_DATA_EXTRACTION)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

