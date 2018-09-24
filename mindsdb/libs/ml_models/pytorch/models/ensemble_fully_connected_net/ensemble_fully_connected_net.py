from mindsdb.config import *
from mindsdb.libs.constants.mindsdb import *

import torch
import math
import torch.nn as nn


from mindsdb.libs.ml_models.pytorch.libs.base_model import BaseModel



class EnsembleFullyConnectedNet(BaseModel):

    #ignore_types = [] # NONE

    def setup(self, sample_batch):
        """
        Here we define the basic building blocks of our model, in forward we define how we put it all together along wiht an input

        :param sample_batch: this is used to understand the characteristics of the input and target, it is an object of type utils.libs.data_types.batch.Batch
        """

        self.flatTarget = True # True means that we will expect target to be a flat vector per row, even if its multiple variables
        self.flatInput = False # True means that we will expect input to be a dictionary of flat vectors per column

        sample_input = sample_batch.getInput(flatten=self.flatInput)
        sample_target = sample_batch.getTarget(flatten=self.flatTarget)
        output_size = sample_target.size()[1]

        self.nets = {

            col:  nn.Sequential(
                nn.Linear(sample_input[col].size()[1], 6),
                torch.nn.LeakyReLU(),
                nn.Linear(6, 3),
                torch.nn.LeakyReLU(),
                nn.Linear(3, output_size)
            )

            for col in sample_input }

        self.ordered_cols = [col for col in self.nets]


        # the input of the last neural net is the flat input + all the predictions per column
        input2_size = len(sample_input)*output_size #+ sum([sample_input[col].size()[1] for col in sample_input])


        self.regnet = nn.Sequential(
            nn.Linear(input2_size, int(math.ceil(input2_size/2))),
            torch.nn.LeakyReLU(),
            nn.Linear(int(math.ceil(input2_size/2)), output_size)
        )

        if USE_CUDA:
            self.regnet.cuda()
            for col in self.nets:
                self.nets[col].cuda()

    def calculateBatchLoss(self, batch):
        """

        :param batch:
        :return:
        """

        predicted_target, inner_preds = self.forward(batch.getInput(flatten=self.flatInput), True)
        real_target = batch.getTarget(flatten=self.flatTarget)
        loss = 0
        for pred in inner_preds:
            loss += self.lossFunction(pred, real_target)

        loss = loss/len(inner_preds)

        loss += self.lossFunction(predicted_target, real_target)
        batch_size = real_target.size()[0]
        return loss, batch_size



    def forward(self, input, return_inner_outputs = False):
        """
        In this particular model, we just need to forward the network defined in setup, with our input

        :param input: a pytorch tensor with the input data of a batch
        :return:
        """
        inner_outputs = [self.nets[col](input[col]) for col in self.ordered_cols]


        base_outputs = tuple(inner_outputs)
        input_tensor = torch.cat(base_outputs,1)
        output = self.regnet(input_tensor)

        if return_inner_outputs == True:
            return output, inner_outputs
        else:
            return output






