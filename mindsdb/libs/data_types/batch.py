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

from mindsdb.libs.constants.mindsdb import *
import numpy as np

class Batch:
    def __init__(self, sampler, data_dict, mirror = False, group=None, column=None, start=None, end=None):
        """

        :param sampler: The object generating batches
        :type sampler: libs.data_types.sampler.Sampler
        :param data_dict: the actual data
        :param mirror: if you want input and target to be the same
        """
        self.data_dict = data_dict
        self.sampler = sampler
        self.mirror = mirror
        self.number_of_rows = None
        self.blank_columns = []

        # these are pointers to trace it back to the original data
        self.group_pointer = group
        self.column_pointer = column
        self.start_pointer = start
        self.end_pointer = end

        # some usefule variables, they can be obtained from sambpler and metada but its a pain, so we keep them here
        self.target_column_names = self.sampler.meta_data.predict_columns
        self.input_column_names = [col for col in self.sampler.model_columns if col not in self.target_column_names]

        # This is the template for the response
        ret = {'input':{}, 'target':{}}

        # this may change as we have more targets
        # TODO: if the target is * how do we go about it here
        # Ideas: iterate over the missing column

        # populate the ret dictionary,
        # where there are inputs and targets and each is a dictionary
        # with key name being the column and the value the column data
        for col in self.sampler.model_columns:
            # this is to populate always in same order
            if col not in self.data_dict:
                continue

            if self.mirror:
                ret['target'][col] = self.data_dict[col]
                ret['input'][col] = self.data_dict[col]

            elif col in self.sampler.meta_data.predict_columns:
                ret['target'][col] = self.data_dict[col]
            else:
                ret['input'][col] = self.data_dict[col]

            if self.number_of_rows is None:
                self.number_of_rows = self.data_dict[col][0].size


        self.xy = ret

        return

    def getColumn(self, what, col):
        if col in self.blank_columns:
            return np.zeros_like(self.xy[what][col])
        return self.xy[what][col]

    def get(self, what, flatten = True):
        ret = None
        if flatten:
            # make sure we serialize in the same order that input metadata columns
            for col in self.sampler.model_columns:
                if col not in self.xy[what]:
                    continue
                # make sure that this is always in the same order, use a list or make xw[what] an ordered dictionary
                if ret is None:
                    ret = self.getColumn(what,col)
                else:
                    ret = np.concatenate((ret, self.getColumn(what,col)), axis=1)

            if self.sampler.variable_wrapper is not None:
                return self.sampler.variable_wrapper(ret)
            else:
                return ret

        if self.sampler.variable_wrapper is not None:
            ret = {}
            for col in self.xy[what]:
                ret[col] = self.sampler.variable_wrapper(self.getColumn(what,col))
            return ret
        else:
            return self.xy[what]

    def getInput(self, flatten = True):
        return self.get('input', flatten)


    def getTarget(self, flatten = True):
        return self.get('target', flatten)

    def deflatTarget(self, flat_vector):
        ret = {}
        start = 0
        for col in self.sampler.model_columns:
            # this is to populate always in same order

            if col in self.sampler.meta_data.predict_columns:
                end = self.data_dict[col].shape[1] # get when it ends
                ret[col] = flat_vector[:,start:end]
                start = end

        return ret

    def getInputStats(self):

        stats = {}

        for col in self.sampler.meta_data.predict_columns:
            stats[col] = self.sampler.stats[col]

        return stats

    def getTargetStats(self):

        stats = {}

        for col in self.sampler.model_columns:
            if col not in self.sampler.meta_data.predict_columns:
                stats[col] = self.sampler.stats[col]

        return stats


    def size(self):
        return self.number_of_rows







