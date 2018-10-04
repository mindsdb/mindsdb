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
import itertools
import logging
import traceback

from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from collections import OrderedDict
from mindsdb.libs.helpers.norm_denorm_helpers import norm
from mindsdb.libs.helpers.text_helpers import hashtext
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata


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


    def _getRowExtraVector(self, ret, column_name, col_row_index, distances):

        predict_columns = self.train_meta_data.model_predict_columns

        desired_total = self.train_meta_data.window_size
        batch_height = len(ret[column_name])
        remaining_row_count = batch_height - (col_row_index +1)


        harvest_count = desired_total if desired_total < remaining_row_count else remaining_row_count
        empty_count = desired_total - harvest_count
        empty_vector_len = (
                len(ret[column_name][col_row_index])
                + sum( [ len(ret[predict_col_name][0]) for predict_col_name in predict_columns])
                + 1) * empty_count # this is the width of the padding


        row_extra_vector = []

        for i in range(harvest_count):
            try:
                row_extra_vector += ret[column_name][col_row_index + i + 1]
                row_extra_vector += [distances[col_row_index + i+1]]

                # append the target values before:
                for predict_col_name in predict_columns:
                     row_extra_vector += [float(v) for v in ret[predict_col_name][col_row_index + i + 1]]
            except:
                logging.error(traceback.format_exc())
                logging.error('something is not right, seems like we got here with np arrays and they should not be!')

        if empty_count > 0:
            # complete with empty
            row_extra_vector += [0] *  empty_vector_len

        return row_extra_vector




    def run(self):



        self.train_meta_data = TransactionMetadata()
        self.train_meta_data.setFromDict(self.transaction.persistent_model_metadata.train_metadata)

        group_by = self.train_meta_data.model_group_by

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

            target_set = group['target_set'] # for ease use a pointer

            # iterate over all indexes taht belong to this group
            for input_row_index in group['indexes']:

                row = self.transaction.input_data.data_array[input_row_index] # extract the row from input data
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

                distances = None

                # if we have a group by and order by calculate a distances vector for each data point in this batch
                if self.train_meta_data.model_group_by is not None and self.train_meta_data.model_order_by is not None:
                    distances = []
                    batch_height = len(target_set[group_by_hash][self.train_meta_data.model_group_by])

                    # create a vector for the top distance

                    for j in range(batch_height):
                        order_by_bottom_vector = np.array(
                            list(itertools.chain.from_iterable(
                                [target_set[group_by_hash][order_by_col][j] for order_by_col in
                                 self.train_meta_data.model_order_by]
                            ))
                        )
                        if j == 0:
                            order_by_top_vector = order_by_bottom_vector
                        else:
                            order_by_top_vector = np.array(list(itertools.chain.from_iterable(
                                [target_set[group_by_hash][order_by_col][j - 1] for order_by_col in
                                 self.train_meta_data.model_order_by])))
                        # create a vector for the current row

                        # calculate distance and append to distances
                        distance = float(np.linalg.norm(order_by_top_vector - order_by_bottom_vector))
                        distances.append(distance)

                # Append the time series data to each column
                # NOTE: we want to make sure that the self.train_meta_data.model_predict_columns are the first in being converted into vectors
                #       the reason for this is that if there is a time series query then we will want to add the history of the target value (see self._getRowExtraVector)
                columns_in_order = self.train_meta_data.model_predict_columns + [column_name for column_name in target_set[group_by_hash] if column_name not in self.train_meta_data.model_predict_columns]

                for column_name in columns_in_order:

                    # if there is a group by and order by and this is not a column to be predicted, append history vector
                    # TODO: Encode the history vector if possible
                    non_groupable_columns = self.train_meta_data.model_predict_columns + [self.train_meta_data.model_group_by] +  self.train_meta_data.model_order_by
                    # NOTE: since distances is only not None if there is a group by this is only evaluated for group by queries
                    if distances is not None and column_name not in non_groupable_columns:
                        # for each row create a vector of history and append to it
                        prev = 0
                        for col_row_index, col_row in enumerate(target_set[group_by_hash][column_name]):
                            row_extra_vector = self._getRowExtraVector(target_set[group_by_hash], column_name, col_row_index, distances)
                            target_set[group_by_hash][column_name][col_row_index] = target_set[group_by_hash][column_name][col_row_index]+ row_extra_vector


                    target_set[group_by_hash][column_name] = np.array(target_set[group_by_hash][column_name])



        return []


def test():
    from mindsdb.libs.controllers.mindsdb_controller import MindsDBController as MindsDB

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
