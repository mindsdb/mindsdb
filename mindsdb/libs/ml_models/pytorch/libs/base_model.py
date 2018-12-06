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

import torch
import torch.nn as nn
from torch import optim
from torch.autograd import Variable


from sklearn.metrics import r2_score, explained_variance_score

import numpy as np

from mindsdb.config import USE_CUDA
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.ml_models.pytorch.libs.torch_helpers import arrayToFloatVariable, variableToArray
from mindsdb.libs.ml_models.pytorch.libs.torch_helpers import getTorchObjectBinary, storeTorchObject, getStoredTorchObject, RMSELoss, LogLoss

from mindsdb.libs.data_types.trainer_response import TrainerResponse
from mindsdb.libs.data_types.tester_response import TesterResponse
from mindsdb.libs.data_types.file_saved_response import FileSavedResponse
from mindsdb.libs.helpers.norm_denorm_helpers import denorm
from mindsdb.libs.helpers.train_helpers import getColPermutations, getAllButOnePermutations, getOneColPermutations

import random

class BaseModel(nn.Module):

    variable_wrapper = arrayToFloatVariable
    variable_unwrapper = variableToArray
    ignore_types = [DATA_TYPES.FULL_TEXT]
    use_full_text_input = False

    def __init__(self, sample_batch, **kwargs):
        """

        :param sample_batch:
        :type sample_batch: utils.
        :param use_cuda:
        :param kwargs:
        """
        super(BaseModel, self).__init__()

        self.lossFunction = LogLoss()
        self.errorFunction = LogLoss()
        self.sample_batch = sample_batch

        # Implementing this
        # https://towardsdatascience.com/understanding-learning-rates-and-how-it-improves-performance-in-deep-learning-d0d4059c1c10
        self.learning_rates = [(1, 200), (0.8, 20), (0.6, 20),(0.4, 20), (0.2, 40),(0.1, 60), (0.01, 80), (0.001, 100)]
        for i in range(5):
            self.learning_rates += [(1, 40), (0.8, 20), (0.6, 20),(0.4, 20), (0.2, 20),(0.1, 20), (0.01, 30), (0.001, 40)]
        for i in range(5):
            self.learning_rates += [(0.1, 60), (0.08, 30), (0.06, 30),(0.04, 20), (0.02, 30),(0.01, 30), (0.005, 40), (0.001, 50)]


        self.setLearningRateIndex(0)

        self.latest_file_id = None

        self.flatTarget = True
        self.flatInput = True
        self.optimizer = None

        self.current_accuracy = 0

        self.optimizer_class_list = [optim.ASGD] #, optim.Adam] # this is (limit on accuracy, and optimizer)
        self.optimizer_class = self.optimizer_class_list[0]

        self.setup(sample_batch,  **kwargs)

        # extract all possible meta data from sample batch so it is no longer needed in future
        self.target_column_names = self.sample_batch.target_column_names
        self.input_column_names =  self.sample_batch.input_column_names

        # column permutations for learning Nones
        self.col_permutations =  getOneColPermutations(self.input_column_names) #+ [[]]


    def zeroGradOptimizer(self):
        """
        Optimizers are reset every n epochs, to make sure that we walk as much of the random state of the weights
        We do this to try to ensure that models converge in the same minimas, and avoid saddle points

        :return: None
        """


        if self.optimizer is None:

            # see if we need to change the optimizer class
            # Note: change happens when the accuracy is greater than specified in the self.optimizer_class_list
            next_index = 0

            if self.current_accuracy !=0 :
                current_index = self.optimizer_class_list.index(self.optimizer_class)

                if current_index + 1 >= len(self.optimizer_class_list):
                    next_index = 0
                else:
                    next_index = current_index + 1

            self.optimizer_class = self.optimizer_class_list[next_index]
            # initialize new optimizer
            self.optimizer = self.optimizer_class(self.parameters(), lr=self.current_learning_rate)

        # very important
        self.optimizer.zero_grad()


    def setLearningRateIndex(self, index):
        """
        This updates the pointers in the learning rates
        :param index: the index
        :return:
        """
        if index >= len(self.learning_rates):
            index = len(self.learning_rates) -1
            logging.warning('Trying to set the learning rate on an index greater than learnign rates available')

        self.current_learning_rate_index = index
        self.total_epochs = self.learning_rates[self.current_learning_rate_index][EPOCHS_INDEX]
        self.current_learning_rate = self.learning_rates[self.current_learning_rate_index][LEARNING_RATE_INDEX]

    def optimize(self):
        """

        :return:
        """

        if self.optimizer is None:
            self.optimizer = self.optimizer_class(self.parameters(), lr=self.current_learning_rate)
        self.optimizer.step()

    def forwardWrapper(self, batch):
        if self.use_full_text_input == False:
            return self.forward(batch.getInput(flatten=self.flatInput))
        else:
            return self.forward(batch.getInput(flatten=self.flatInput), full_text_input = batch.getFullTextInput())

    def calculateBatchLoss(self, batch):
        """

        :param batch:
        :return:
        """

        predicted_target = self.forwardWrapper(batch)
        real_target = batch.getTarget(flatten=self.flatTarget)
        loss = self.lossFunction(predicted_target, real_target)
        batch_size = real_target.size()[0]
        return loss, batch_size


    def saveToDisk(self, file_id = None):
        """

        :return:
        """
        sample_batch = self.sample_batch
        self.sample_batch = None
        file_id, path = storeTorchObject(self, file_id)
        self.latest_file_id = file_id
        self.sample_batch = sample_batch
        return [FileSavedResponse(file_id, path)]

    @staticmethod
    def loadFromDisk(file_ids):
        """

        :param file_ids:
        :return:
        """
        obj = getStoredTorchObject(file_ids[0])
        obj.eval()
        return obj


    def getLatestFromDisk(self):
        """

        :return:
        """
        obj = getStoredTorchObject(self.latest_file_id)
        obj.eval()
        return obj


    def testModel(self, test_sampler):
        """

        :param test_sampler:
        :return:  TesterResponse
        """


        real_target_all = []
        predicted_target_all = []

        real_target_all_ret = []
        predicted_target_all_ret = []

        self.eval() # toggle eval
        perm_index = 0

        for batch_number, batch in enumerate(test_sampler):

            # do only one permutation at a time, if we 2 or more columns
            if len(self.input_column_names) > 1:
                perms = [self.col_permutations[perm_index], []]
                perm_index = perm_index + 1 if perm_index + 1 < len(self.col_permutations) else 0
            else:
                perms = [[]]

            for permutation in perms:
                batch.blank_columns = permutation
                #batch.blank_columns = []
                logging.debug('[EPOCH-BATCH] testing batch: {batch_number}'.format(batch_number=batch_number))
                # get real and predicted values by running the model with the input of this batch
                predicted_target = self.forwardWrapper(batch)
                real_target = batch.getTarget(flatten=self.flatTarget)
                # append to all targets and all real values
                real_target_all += real_target.data.tolist()
                predicted_target_all += predicted_target.data.tolist()

                if len(permutation) == 0:
                    # append to all targets and all real values
                    real_target_all_ret += real_target.data.tolist()
                    predicted_target_all_ret += predicted_target.data.tolist()

        if batch is None:
            logging.error('there is no data in test, we should not be here')
            return

        # caluclate the error for all values
        predicted_targets = batch.deflatTarget(np.array(predicted_target_all))
        real_targets = batch.deflatTarget(np.array(real_target_all))
        # caluclate the error for all values
        predicted_targets_ret = batch.deflatTarget(np.array(predicted_target_all_ret))
        real_targets_ret = batch.deflatTarget(np.array(real_target_all_ret))

        r_values = {}
        # calculate r and other statistical properties of error
        for target_key in real_targets_ret:

            r_values[target_key] =  explained_variance_score(real_targets_ret[target_key], predicted_targets_ret[target_key], multioutput='variance_weighted')


        # calculate error using error function
        errors = {target_key: float(self.errorFunction(Variable(torch.FloatTensor(predicted_targets[target_key])), Variable(torch.FloatTensor(real_targets[target_key]))).item()) for target_key in real_targets}
        error = np.average([errors[key] for key in errors])
        r_value = np.average([r_values[key] for key in r_values])


        resp = TesterResponse(
            error = error,
            accuracy= r_value,
            predicted_targets = predicted_targets_ret,
            real_targets = real_targets_ret
        )

        self.current_accuracy = r_value

        return resp


    def trainModel(self, train_sampler, learning_rate_index = None):
        """
        This function is an interator to train over the sampler

        :param train_sampler: the sampler to iterate over and train on

        :yield: TrainerResponse
        """



        model_object = self
        response = TrainerResponse(model_object)

        if learning_rate_index is not None:
            self.setLearningRateIndex(learning_rate_index)

        model_object.optimizer = None

        for epoch in range(self.total_epochs):

            full_set_loss = 0
            total_samples = 0
            response.epoch = epoch
            perm_index = 0
            # train epoch
            for batch_number, batch in enumerate(train_sampler):
                # TODO: Build machanics for model to learn about missing data
                # How? Here build permutation list of all possible combinations of blank columns
                # Iterate over permutations on train loop (which is what is inside this for statement)
                # Interface: Batch.setNullColumns(cols=<type: list>)

                # do only one permutation at a time, if we 2 or more columns
                if len(self.input_column_names) > 1:
                    perms = [self.col_permutations[perm_index], []]
                    perm_index = perm_index +1 if perm_index + 1 < len(self.col_permutations) else 0
                else:
                    perms = [[]]
                # so here we pass one column per batch that can use as permutation,
                # essentially on every batch it makes sure that we pass all columns [[]] and also we do a run with one column with none values

                for permutation in perms:
                    batch.blank_columns = permutation
                    response.batch = batch_number
                    logging.debug('[EPOCH-BATCH] Training on epoch: {epoch}/{num_epochs}, batch: {batch_number}'.format(
                            epoch=epoch + 1, num_epochs=self.total_epochs, batch_number=batch_number))
                    model_object.train() # toggle to train
                    model_object.zeroGradOptimizer()
                    loss, batch_size = model_object.calculateBatchLoss(batch)
                    if batch_size <= 0:
                        break
                    total_samples += batch_size
                    full_set_loss += int(loss.item()) * batch_size # this is because we need to wight the error by samples in batch
                    average_loss = full_set_loss / total_samples
                    loss.backward()#retain_graph=True)
                    model_object.optimize()
                    response.loss = average_loss

                    yield response



    # #############
    # METHODS TO IMPLEMENT BY CHILDREN
    # #############

    def setup(self, sample_batch, **kwargs):
        """
        this is what is called when the model object is instantiated
        :param sample_batch:
        :param use_cuda:
        :return:
        """
        logging.error('You must define a setup method for this model')
        pass

    def forward(self, input):
        """
        This is what is called when the model is forwarded
        :param input:
        :return:
        """
        logging.error('You must define a forward method for this model')
        pass





