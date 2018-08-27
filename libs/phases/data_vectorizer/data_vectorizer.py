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


import copy
import numpy as np


from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from collections import OrderedDict
from libs.helpers.norm_denorm_helpers import norm
from libs.helpers.text_helpers import hashtext


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


    def run(self):

        group_by = self.transaction.metadata.model_group_by
        group_by_index = None
        if group_by:
            group_by_index = self.transaction.input_data.columns.index(group_by)  # TODO: Consider supporting more than one index column

        # this is a template of how we store columns
        column_packs_template = OrderedDict()

        # create a template of the column packs
        for i, column_name in enumerate(self.transaction.input_data.columns):
            column_packs_template[column_name] = []

        if self.transaction.metadata.type == TRANSACTION_LEARN:
            groups = [
                {
                    'name': 'test',
                    'target_set': self.transaction.model_data.test_set,
                    'map': self.transaction.model_data.test_set_map,
                    'indexes': self.transaction.input_data.test_indexes
                },
                {
                    'name': 'train',
                    'target_set': self.transaction.model_data.train_set,
                    'map': self.transaction.model_data.train_set_map,
                    'indexes': self.transaction.input_data.train_indexes
                },
                {
                    'name': 'validation',
                    'target_set': self.transaction.model_data.validation_set,
                    'map': self.transaction.model_data.validation_set_map,
                    'indexes': self.transaction.input_data.validation_indexes
                }
            ]
        else:
            groups = [
                {
                    'name': 'predict',
                    'target_set': self.transaction.model_data.predict_set,
                    'map': self.transaction.model_data.predict_set_map,
                    'indexes': range(0,len(self.transaction.input_data.data_array)) # TODO: measure impact of this
                }
            ]

        # iterate over all groups and populate tensors by columns

        for group in groups:

            # iterate over all indexes taht belong to this group
            for input_row_index in group['indexes']:

                row = self.transaction.input_data.data_array[input_row_index] # extract the row from input data
                target_set = group['target_set'] # for ease use a pointer
                map = group['map']

                if group_by is not None:
                    group_by_hash = hashtext(row[group_by_index])

                else:
                    group_by_hash = KEY_NO_GROUP_BY

                # if the set group has not been initiated add a new one
                if group_by_hash not in target_set:
                    target_set[group_by_hash] = copy.deepcopy(column_packs_template)
                    map[group_by_hash] = {}


                # Now populate into the group_hash column pile
                for column_index, cell_value in enumerate(row):

                    column_name = self.transaction.input_data.columns[column_index]

                    value = self.cast(cell_value)
                    stats = self.transaction.persistent_model_metadata.column_stats[column_name]

                    # TODO: Provide framework for custom nom functions
                    # TODO: FIX norm allways add column for is null
                    normalized = norm(value=value, cell_stats=stats) # this should return a vector representation already normalized

                    # keep track of where it came from in the input data inc ase we need to go back
                    position = len(target_set[group_by_hash][column_name])
                    map[group_by_hash][position] = input_row_index

                    # append normalized vector to column tensor
                    target_set[group_by_hash][column_name] += [normalized]


            # turn into numpy arrays:
            for group_by_hash in target_set:
                for column_name in target_set[group_by_hash]:
                    target_set[group_by_hash][column_name] = np.array(target_set[group_by_hash][column_name])



        return []


def test():
    from libs.controllers.mindsdb_controller import MindsDBController as MindsDB

    mdb = MindsDB()
    mdb.learn(
        from_query='select * from position_target_table',
        group_by='id',
        order_by=['max_time_rec'],
        predict='position',
        model_name='mdsb_model',
        breakpoint=PHASE_DATA_VECTORIZATION
    )



# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
