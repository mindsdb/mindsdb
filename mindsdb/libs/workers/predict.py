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

#import logging
from mindsdb.libs.helpers.logging import logging

from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string, get_label_index_for_value
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.helpers.norm_denorm_helpers import denorm

from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_entities.persistent_ml_model_info import PersistentMlModelInfo

import importlib
import time


class PredictWorker():
    def __init__(self, model_name, data=None):
        """
        Load basic data needed to find the model data
        :param data: data to make predictions on
        :param model_name: the model to load
        :param submodel_name: if its also a submodel, the submodel name
        """

        self.model_name = model_name
        self.data = data
        self.persistent_model_metadata = PersistentModelMetadata()
        self.persistent_model_metadata.model_name = self.model_name
        self.persistent_ml_model_info = PersistentMlModelInfo()
        self.persistent_ml_model_info.model_name = self.model_name

        self.persistent_model_metadata = self.persistent_model_metadata.find_one(self.persistent_model_metadata.getPkey())

        # laod the most accurate model

        info = self.persistent_ml_model_info.find({'model_name': self.model_name}, order_by=[('r_squared', -1)], limit=1)

        if info is not None and len(info) > 0:
            self.persistent_ml_model_info = info[0] #type: PersistentMlModelInfo
        else:
            # TODO: Make sure we have a model for this
            logging.info('No model found')
            return

        self.ml_model_name = self.persistent_ml_model_info.ml_model_name
        self.config_serialized = self.persistent_ml_model_info.config_serialized

        fs_file_ids = self.persistent_ml_model_info.fs_file_ids
        self.framework, self.dummy, self.ml_model_name = self.ml_model_name.split('.')
        self.ml_model_module_path = 'mindsdb.libs.ml_models.' + self.framework + '.models.' + self.ml_model_name + '.' + self.ml_model_name
        self.ml_model_class_name = convert_snake_to_cammelcase_string(self.ml_model_name)

        self.ml_model_module = importlib.import_module(self.ml_model_module_path)
        self.ml_model_class = getattr(self.ml_model_module, self.ml_model_class_name)

        self.gfs_save_head_time = time.time()  # the last time it was saved into GridFS, assume it was now

        logging.info('Starting model...')
        self.data_model_object = self.ml_model_class.loadFromDisk(file_ids=fs_file_ids)

        if self.data != None:
            self._loadData(data)


    def _loadData(self, data):
        """
        Load data
        :param data:
        :return:
        """
        self.data = data
        self.predict_sampler = Sampler(self.data.predict_set, metadata_as_stored=self.persistent_model_metadata)
        self.sample_batch = self.predict_sampler.getSampleBatch()
        self.data_model_object.sample_batch = self.sample_batch


    def predict(self, data=None):
        """
        This actually calls the model and returns the predictions in diff form

        :return: diffs, which is a list of dictionaries with pointers as to where to replace the prediction given the value that was predicted

        """

        if data != None:
            self._loadData(data)

        self.predict_sampler.variable_wrapper = self.ml_model_class.variable_wrapper
        self.predict_sampler.variable_unwrapper = self.ml_model_class.variable_unwrapper

        ret_diffs = []
        for batch in self.predict_sampler:

            logging.info('predicting batch...')
            if self.data_model_object.use_full_text_input:
                ret = self.data_model_object.forward(batch.getInput(flatten=self.data_model_object.flatInput), full_text_input=batch.getFullTextInput())
            else:
                ret = self.data_model_object.forward(batch.getInput(flatten=self.data_model_object.flatInput))
            if type(ret) != type({}):
                ret_dict = batch.deflatTarget(ret)
            else:
                ret_dict = ret

            ret_dict_denorm = {}


            for col in ret_dict:
                ret_dict[col] = self.ml_model_class.variable_unwrapper(ret_dict[col])
                for row in ret_dict[col]:
                    if col not in ret_dict_denorm:
                        ret_dict_denorm[col] = []

                    ret_dict_denorm[col] += [denorm(row, self.persistent_model_metadata.column_stats[col])]


            ret_total_item = {
                'group_pointer': batch.group_pointer,
                'column_pointer': batch.column_pointer,
                'start_pointer': batch.start_pointer,
                'end_pointer': batch.end_pointer,
                'ret_dict': ret_dict_denorm
            }
            ret_diffs += [ret_total_item]

        return ret_diffs



    @staticmethod
    def start(model_name, data):
        """
        We use this worker to parallel train different data models and data model configurations

        :param data: This is the vectorized data
        :param model_name: This will be the model name so we can pull stats and other
        :param data_model: This will be the data model name, which can let us find the data model implementation
        :param config: this is the hyperparameter config
        """

        w = PredictWorker(model_name)
        logging.info('Inferring from model and data...')
        return w.predict(data)

