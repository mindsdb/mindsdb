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

# import logging
from mindsdb.libs.helpers.logging import logging


from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string, get_label_index_for_value
from mindsdb.libs.helpers.text_helpers import hashtext
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.helpers.norm_denorm_helpers import denorm

from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_entities.persistent_ml_model_info import PersistentMlModelInfo
from mindsdb.libs.data_types.model_data import ModelData

import importlib
import json
import time
import os
import copy

class TrainWorker():
    
    def __init__(self, data, model_name, ml_model_name='pytorch.models.column_based_fcnn', config={}):
        """

        :param data:
        :type data: ModelData
        :param model_name:
        :param ml_model_name:
        :param config:
        """

        self.data = data
        self.model_name = model_name
        self.ml_model_name = ml_model_name
        self.config = config
        self.config_serialized = json.dumps(self.config)
        self.config_hash = hashtext(self.config_serialized)

        # get basic variables defined

        self.persistent_model_metadata = PersistentModelMetadata().find_one({'model_name': self.model_name})

        self.ml_model_info = PersistentMlModelInfo()
        self.ml_model_info.model_name = self.model_name
        self.ml_model_info.ml_model_name = self.ml_model_name
        self.ml_model_info.config_serialized = self.config_serialized
        self.ml_model_info.insert()

        self.framework, self.dummy, self.data_model_name = self.ml_model_name.split('.')
        self.ml_model_module_path = 'mindsdb.libs.ml_models.' + self.ml_model_name + '.' + self.data_model_name
        self.ml_model_class_name = convert_snake_to_cammelcase_string(self.data_model_name)

        self.ml_model_module = importlib.import_module(self.ml_model_module_path)
        self.ml_model_class = getattr(self.ml_model_module, self.ml_model_class_name)

        self.train_sampler = Sampler(self.data.train_set, metadata_as_stored=self.persistent_model_metadata, ignore_types=self.ml_model_class.ignore_types, sampler_mode=SAMPLER_MODES.LEARN)
        self.test_sampler = Sampler(self.data.test_set, metadata_as_stored=self.persistent_model_metadata, ignore_types=self.ml_model_class.ignore_types, sampler_mode=SAMPLER_MODES.LEARN)

        self.train_sampler.variable_wrapper = self.ml_model_class.variable_wrapper
        self.test_sampler.variable_wrapper = self.ml_model_class.variable_wrapper
        self.sample_batch = self.train_sampler.getSampleBatch()

        self.gfs_save_head_time = time.time() # the last time it was saved into GridFS, assume it was now

        logging.info('Starting model...')
        self.data_model_object = self.ml_model_class(self.sample_batch)
        logging.info('Training model...')
        self.train()

        
    def train(self):
        """

        :return:
        """

        last_epoch = 0
        lowest_error = None
        local_files = None

        for i in range(len(self.data_model_object.learning_rates)):

            self.data_model_object.setLearningRateIndex(i)

            for train_ret in self.data_model_object.trainModel(self.train_sampler):

                logging.debug('Training State epoch:{epoch}, batch:{batch}, loss:{loss}'.format(epoch=train_ret.epoch,
                                                                                               batch=train_ret.batch,
                                                                                               loss=train_ret.loss))

                # save model every new epoch
                if last_epoch != train_ret.epoch:
                    last_epoch = train_ret.epoch
                    logging.debug('New epoch:{epoch}, testing and calculating error'.format(epoch=last_epoch))
                    test_ret = self.data_model_object.testModel(self.test_sampler)
                    logging.info('Test Error:{error}, Accuracy:{accuracy}'.format(error=test_ret.error, accuracy=test_ret.accuracy))
                    is_it_lowest_error_epoch = False
                    # if lowest error save model
                    if lowest_error in [None]:
                        lowest_error = test_ret.error
                    if lowest_error > test_ret.error:
                        is_it_lowest_error_epoch = True
                        lowest_error = test_ret.error
                        logging.info('[SAVING MODEL] Lowest ERROR so far! - Test Error: {error}, Accuracy: {accuracy}'.format(error=test_ret.error, accuracy=test_ret.accuracy))
                        logging.debug('Lowest ERROR so far! Saving: model {model_name}, {data_model} config:{config}'.format(
                            model_name=self.model_name, data_model=self.ml_model_name, config=self.ml_model_info.config_serialized))

                        # save model local file
                        local_files = self.saveToDisk(local_files)
                        # throttle model saving into GridFS to 10 minutes
                        # self.saveToGridFs(local_files, throttle=True)

                        # save model predicted - real vectors
                        logging.debug('Saved: model {model_name}:{ml_model_name} state vars into db [OK]'.format(model_name=self.model_name, ml_model_name = self.ml_model_name))

                    # check if continue training
                    if self.shouldContinue() == False:
                        return
                    # save/update model loss, error, confusion_matrix
                    self.registerModelData(train_ret, test_ret, is_it_lowest_error_epoch)

            logging.info('Loading model from store for retrain on new learning rate {lr}'.format(lr=self.data_model_object.learning_rates[i][LEARNING_RATE_INDEX]))
            # after its done with the first batch group, get the one with the lowest error and keep training

            ml_model_info = self.ml_model_info.find_one({
                'model_name': self.model_name,
                'ml_model_name': self.ml_model_name,
                'config_serialized': json.dumps(self.config)
            })


            if ml_model_info is None:
                # TODO: Make sure we have a model for this
                logging.info('No model found in storage')
                return

            fs_file_ids = ml_model_info.fs_file_ids

            self.data_model_object = self.ml_model_class.loadFromDisk(file_ids=fs_file_ids)




        # When out of training loop:
        # - if stop or finished leave as is (TODO: Have the hability to stop model training, but not necessarily delete it)
        #   * save best lowest error into GridFS (we only save into GridFS at the end because it takes too long)
        #   * remove local model file
        # self.saveToGridFs(local_files=local_files, throttle=False)


    def registerModelData(self, train_ret, test_ret, lowest_error_epoch = False):
        """
        This method updates stats about the model, it's called on each epoch

        Stores:
            - loss
            - error
            - confusion matrices

        :param train_ret    The result of training a batch
        :param test_ret     The result of testing after an epoch
        :param lowest_error_epoch   Is this epoch the one with the lowest error so far
        """


        # Operations that happen regardless of it being or not a lowest error epoch or not

        self.ml_model_info.loss_y += [train_ret.loss]
        self.ml_model_info.loss_x += [train_ret.epoch]
        self.ml_model_info.error_y += [test_ret.error]
        self.ml_model_info.error_x += [train_ret.epoch]

        if lowest_error_epoch == True:

            # denorm the real and predicted
            predicted_targets = {}
            real_targets = {}
            for col in test_ret.predicted_targets:
                predicted_targets[col] = [denorm(row, self.persistent_model_metadata.column_stats[col]) for row in test_ret.predicted_targets[col]]
                real_targets[col] = [denorm(row, self.persistent_model_metadata.column_stats[col]) for row in test_ret.real_targets[col]]


            self.ml_model_info.confussion_matrices = self.calculateConfusionMatrices(real_targets, predicted_targets)
            self.ml_model_info.lowest_error = test_ret.error
            self.ml_model_info.predicted_targets = predicted_targets
            self.ml_model_info.real_targets = real_targets
            self.ml_model_info.accuracy = test_ret.accuracy
            self.ml_model_info.r_squared = test_ret.accuracy



        self.ml_model_info.update()


        return True


    def calculateConfusionMatrices(self, real_targets, predicted_targets):
        """
        This calcilates confusion matrices for the realx_predicted

        :param real_targets:
        :param predicted_targets:
        TODO: Make this logarithmic confussion matrix for NUMERIC types

        :return: a dictionary with the confusion matrices, with info as ready as possible to plot

        """
        # confusion matrices with zeros
        confusion_matrices = {
            col: {
                'labels': [label for label in self.persistent_model_metadata.column_stats[col]['histogram']['x']],
                'real_x_predicted_dist': [[0 for i in self.persistent_model_metadata.column_stats[col]['histogram']['x']] for j in
                                          self.persistent_model_metadata.column_stats[col]['histogram']['x']],
                'real_x_predicted': [[0 for i in self.persistent_model_metadata.column_stats[col]['histogram']['x']] for j in
                                     self.persistent_model_metadata.column_stats[col]['histogram']['x']]
            }
            for col in real_targets
        }


        for col in real_targets:
            reduced_buckets = []
            stats = self.persistent_model_metadata.column_stats[col]
            if stats[KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:

                labels = confusion_matrices[col]['labels']
                for i, label in enumerate(labels):
                    index = int(i) + 1
                    if index % 5 == 0:
                        reduced_buckets.append(int(labels[i]))

                reduced_confusion_matrices = {
                    col: {
                        'labels': reduced_buckets,
                        'real_x_predicted_dist': [[0 for i in reduced_buckets] for j in reduced_buckets],
                        'real_x_predicted': [[0 for i in reduced_buckets] for j in reduced_buckets]
                    }
                }
            else:
                #TODO: Smarter way to deal with reduced buckets for other data types
                reduced_buckets = confusion_matrices[col]['labels']
                reduced_confusion_matrices = copy.copy(confusion_matrices)

        # calculate confusion matrices real vs predicted
        for col in predicted_targets:
            totals = [0] * len(self.persistent_model_metadata.column_stats[col]['histogram']['x'])
            reduced_totals = [0] * len(reduced_buckets)
            for i, predicted_value in enumerate(predicted_targets[col]):
                predicted_index = get_label_index_for_value(predicted_value, confusion_matrices[col]['labels'])
                real_index = get_label_index_for_value(real_targets[col][i], confusion_matrices[col]['labels'])
                confusion_matrices[col]['real_x_predicted_dist'][real_index][predicted_index] += 1
                totals[predicted_index] += 1

                reduced_predicted_index = get_label_index_for_value(predicted_value,
                                                                    reduced_confusion_matrices[col]['labels'])
                reduced_real_index = get_label_index_for_value(real_targets[col][i],
                                                               reduced_confusion_matrices[col]['labels'])
                reduced_confusion_matrices[col]['real_x_predicted_dist'][reduced_real_index][
                    reduced_predicted_index] += 1
                reduced_totals[reduced_predicted_index] += 1

            # calculate probability of predicted being correct P(predicted=real|predicted)
            for pred_j, label in enumerate(confusion_matrices[col]['labels']):
                for real_j, label in enumerate(confusion_matrices[col]['labels']):
                    if totals[pred_j] == 0:
                        confusion_matrices[col]['real_x_predicted'][real_j][pred_j] = 0
                    else:
                        confusion_matrices[col]['real_x_predicted'][real_j][pred_j] = \
                        confusion_matrices[col]['real_x_predicted_dist'][real_j][pred_j] / totals[pred_j]

            for pred_j, label in enumerate(reduced_confusion_matrices[col]['labels']):
                for real_j, label in enumerate(reduced_confusion_matrices[col]['labels']):
                    if reduced_totals[pred_j] == 0:
                        reduced_confusion_matrices[col]['real_x_predicted'][real_j][pred_j] = 0
                    else:
                        reduced_confusion_matrices[col]['real_x_predicted'][real_j][pred_j] = \
                        reduced_confusion_matrices[col]['real_x_predicted_dist'][real_j][pred_j] / reduced_totals[
                            pred_j]

        return confusion_matrices

    def shouldContinue(self):
        """
        Check if the training should continue
        :return:
        """

        model_name = self.model_name

        # check if stop training is set in which case we should exit the training

        model_data = self.persistent_model_metadata.find_one({'model_name': self.model_name}) #type: PersistentModelMetadata


        if model_data is None:
            return False

        if model_data.stop_training == True:
            logging.info('[FORCED] Stopping model training....')
            return False

        elif model_data.kill_training == True:

            logging.info('[FORCED] Stopping model training....')
            self.persistent_model_metadata.delete()
            self.ml_model_info.delete()

            return False

        return True

    def saveToDisk(self, local_files):
        """
        This method persists model into disk, and removes previous stored files of this model

        :param local_files: any previous files
        :return:
        """
        if local_files is not None:
            for file_response_object in local_files:
                try:
                    os.remove(file_response_object.path)
                except:
                    logging.info('Could not delete file {path}'.format(path=file_response_object.path))

        file_id = '{model_name}.{ml_model_name}.{config_hash}'.format(model_name=self.model_name, ml_model_name=self.ml_model_name, config_hash=self.config_hash)
        return_objects =  self.data_model_object.saveToDisk(file_id)

        file_ids = [ret.file_id for ret in return_objects]

        self.ml_model_info.fs_file_ids = file_ids
        self.ml_model_info.update()

        return return_objects

    # TODO: Revise if we keep this or not
    # def saveToGridFs(self, local_files, throttle = False):
    #     """
    #     This method is to save to the gridfs local files
    #
    #     :param local_files:
    #     :param throttle:
    #     :return:
    #     """
    #     current_time = time.time()
    #
    #     if throttle == True or local_files is None or len(local_files) == 0:
    #
    #         if (current_time - self.gfs_save_head_time) < 60 * 10:
    #             logging.info('Not saving yet, throttle time not met')
    #             return
    #
    #     # if time met, save to GFS
    #     self.gfs_save_head_time = current_time
    #
    #     # delete any existing files if they exist
    #     model_state = self.mongo.mindsdb.model_state.find_one({'model_name': self.model_name, 'submodel_name': self.submodel_name, 'data_model': self.ml_model_name, 'config': self.config_serialize})
    #     if model_state and 'gridfs_file_ids' in model_state:
    #         for file_id in model_state['gridfs_file_ids']:
    #             try:
    #                 self.mongo_gfs.delete(file_id)
    #             except:
    #                 logging.warning('could not delete gfs {file_id}'.format(file_id=file_id))
    #
    #     file_ids = []
    #     # save into gridfs
    #     for file_response_object in local_files:
    #         logging.info('Saving file into GridFS, this may take a while ...')
    #         file_id = self.mongo_gfs.put(open(file_response_object.path, "rb").read())
    #         file_ids += [file_id]
    #
    #     logging.info('[DONE] files into GridFS saved')
    #     self.mongo.mindsdb.model_state.update_one({'model_name': self.model_name, 'submodel_name': self.submodel_name, 'data_model': self.ml_model_name, 'config': self.config_serialize},
    #                                               {'$set': {
    #                                "model_name": self.model_name,
    #                                'submodel_name': self.submodel_name,
    #                                'data_model': self.ml_model_name,
    #                                'config': self.config_serialize,
    #                                "gridfs_file_ids": file_ids
    #                            }}, upsert=True)

    
    @staticmethod
    def start(data, model_name, ml_model, config={}):
        """
        We use this worker to parallel train different data models and data model configurations
    
        :param data: This is the vectorized data
        :param model_name: This will be the model name so we can pull stats and other
        :param ml_model: This will be the data model name, which can let us find the data model implementation
        :param config: this is the hyperparameter config
        """

        return TrainWorker(data, model_name, ml_model, config)


# TODO: Use ray
# @ray.remote
# def rayRun(**kwargs)
#     TrainWorker.start(**kwargs)