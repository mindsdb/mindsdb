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

import time
import itertools
import numpy as np

# import logging
from mindsdb.libs.helpers.logging import logging

import mindsdb.config as CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.data_types.batch import Batch
from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata

# this implements sampling without replacement and encodes sequential data using encoders
# as described here, its best to sample without replacement:
# https://stats.stackexchange.com/questions/235844/should-training-samples-randomly-drawn-for-mini-batch-training-neural-nets-be-dr

class Sampler:

    def __init__(self, data, metadata_as_stored, batch_size = CONFIG.SAMPLER_MAX_BATCH_SIZE, ignore_types = [],  sampler_mode = SAMPLER_MODES.DEFAULT):
        """

        :param data:
        :param metadata_as_stored:
        :type metadata_as_stored: PersistentModelMetadata
        :param batch_size:
        :param ignore_types:
        """
        self.data = data

        self.meta_data = metadata_as_stored
        self.stats = metadata_as_stored.column_stats
        self.model_columns = [col for col in metadata_as_stored.columns if self.stats[col][KEYS.DATA_TYPE] not in ignore_types]
        self.ignore_columns_with_type = ignore_types
        self.sampler_mode = sampler_mode

        self.batch_size = batch_size
        self.variable_wrapper = None
        self.variable_unwrapper = None



    def getSampleBatch(self):
        """
        Get one single sample batch
        :return:
        """
        for batch in self:
            return batch

    def __iter__(self):
        """

        :return:
        """

        # here we will also determine based on the query if we should do a moving window for the training
        # TODO: if order by encode
        ret = {}

        total_groups = len(self.data)
        for group in self.data:

            group_pointer = 0
            first_column = next(iter(self.data[group]))
            total_length = len(self.data[group][first_column])
            logging.debug('Iterator on group {group}/{total_groups}, total rows: {total_rows}'.format(group=group, total_groups=total_groups, total_rows=total_length))

            while group_pointer < total_length:
                limit = group_pointer + self.batch_size
                limit = limit if limit < total_length else total_length

                allcols_time = time.time()

                for column in self.model_columns:

                    # logging.debug('Generating: pytorch variables, batch: {column}-[{group_pointer}:{limit}]-{column_type}'.format(column=column, group_pointer=group_pointer, limit=limit, column_type=self.stats[column][KEYS.DATA_TYPE]))
                    # col_start_time = time.time()
                    #if self.stats[column][KEYS.DATA_TYPE] != DATA_TYPES.FULL_TEXT:
                    ret[column] = self.data[group][column][group_pointer:limit]

                    # else:
                    #     # Todo: figure out how to deal with full text features here
                    #     ret[column] =[0]*(limit-group_pointer)

                    # logging.debug('Generated: {column} [OK] in {time_delta:.2f} seconds'.format(column=column, time_delta=(time.time()-col_start_time)))

                logging.debug('Generated: [ALL_COLUMNS] in batch [OK], {time_delta:.2f} seconds'.format(time_delta=(time.time() - allcols_time)))

                yield Batch(self, ret, group=group, column=column, start=group_pointer, end=limit )

                ret = {}
                group_pointer = limit



