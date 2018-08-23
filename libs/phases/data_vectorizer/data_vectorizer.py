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

import random as random
import json as json
import hashlib as hashlib
import copy
import numpy as np

from config import *
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from collections import OrderedDict
from libs.helpers.norm_denorm_helpers import norm


class DataVectorizer(BaseModule):

    phase_name = PHASE_DATA_VECTORIZATION

    def cast(self, string):
        """ Returns an integer, float or a string from a string"""
        try:
            if string is None:
                return None
            return int(string)
        except ValueError:
            try:
                return float(string)
            except ValueError:
                if string == '':
                    return None
                else:
                    return string


    def hashCell(self, cell):
        text = json.dumps(cell)
        hash = hashlib.md5(text.encode('utf8')).hexdigest()
        return hash


    def run(self):

        group_by = self.transaction.metadata.model_group_by
        group_by_index = None

        # this is a template of how we store columns
        column_packs_template = OrderedDict()

        for i, column_name in enumerate(self.transaction.persistent_model_metadata.columns):
            if group_by is not None and group_by == column_name:
                group_by_index = i
                # TODO: Consider supporting more than one index column

            column_packs_template[column_name] = []

        # This is used to calculate if its a test based on the existance of self.transaction.input_test_data_array
        # self.transaction.input_test_data_array only exists if the train query contains TEST FROM() statement

        input_data_initial_len = len(self.transaction.input_data.data_array)
        if self.transaction.type == TRANSACTION_LEARN and self.transaction.input_test_data_array:
            self.transaction.input_data_array += self.transaction.input_test_data_array

        for j, row in enumerate(self.transaction.input_data_array):


            if group_by is not None:

                group_by_hash = self.hashCell(row[group_by_index])
                if group_by_hash in self.transaction.model_data[KEYS.TEST_SET]:
                    is_test = True
                elif group_by_hash in self.transaction.model_data[KEYS.TRAIN_SET]:
                    is_test =  False
                else:
                    is_test = True if random.random() <= TEST_TRAIN_RATIO else False

            else:
                group_by_hash = KEY_NO_GROUP_BY
                is_test = True if random.random() <= TEST_TRAIN_RATIO else False

            # if there is a test separate dataset then then j can be greater than initial data len
            if j >= input_data_initial_len:
                is_test = True
            # if there is a separate test dataset but j is still within the train dataset
            elif j < input_data_initial_len and self.transaction.input_test_data_array:
                is_test = False

            # If its a learn transaction ignore is_test variable

            if self.transaction.type == TRANSACTION_LEARN:
                test_train_all = KEYS.TEST_SET if is_test else KEYS.TRAIN_SET
            elif self.transaction.type == TRANSACTION_PREDICT:
                # TODO store values taht should not have predictions in a separate bulk
                test_train_all = KEYS.PREDICT_SET
            else:
                test_train_all = KEYS.ALL_SET

            # place all values in corresponding columns (the reason why we are going for columnar store is because they are easier to operate later)
            # given that operations are the same for elements in the same column we are likely to do more columnar operations
            for column_index, cell_value in enumerate(row):

                column = self.transaction.input_metadata[KEY_COLUMNS][column_index]
                data = self.transaction.model_data[test_train_all]
                value = self.cast(cell_value)
                stats = self.transaction.model_stats[column]

                normalized = norm(value=value, cell_stats=stats)

                if group_by_hash not in data:
                    data[group_by_hash] = copy.deepcopy(column_packs_template)
                    self.transaction.model_data_input_array_map[test_train_all][group_by_hash] = {}

                # keep track of where source data is in respect to vectorized data
                # TODO: see if we can optimize this
                if self.transaction.predict_metadata is not None:
                    position = len(data[group_by_hash][column])
                    if position not in self.transaction.model_data_input_array_map[test_train_all][group_by_hash]:
                        self.transaction.model_data_input_array_map[test_train_all][group_by_hash][position] = j

                data[group_by_hash][column] += [normalized]

                if stats[KEYS.DATA_TYPE] == DATA_TYPES.FULL_TEXT:
                    self.transaction.input_vectors_row_nesting = True

        # turn into numpy arrays:

        for data_group in self.transaction.model_data:
            for group_by_hash in self.transaction.model_data[data_group]:
                for column in self.transaction.model_data[data_group][group_by_hash]:
                    self.transaction.model_data[data_group][group_by_hash][column] = np.array(self.transaction.model_data[data_group][group_by_hash][column])



        return data



def test():
    from libs.test.test_controller import TestController
    module = TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.tweets2) AS tweets2 PREDICT retweet_count ', PHASE_DATA_VECTORIZATION)
    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
