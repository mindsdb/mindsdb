

from mindsdb.config import *
from mindsdb.libs.constants.mindsdb import *

import math
import torch
import torch.nn as nn
from mindsdb.libs.ml_models.pytorch.libs.base_model import BaseModel



class FullyConnectedBucketsNet(BaseModel):


    def setup(self, sample_batch):
        """
        Here we define the basic building blocks of our model, in forward we define how we put it all together along wiht an input

        :param sample_batch: this is used to understand the characteristics of the input and target, it is an object of type utils.libs.data_types.batch.Batch
        """

        self.flatTarget = True # True means that we will expect target to be a flat vector per row, even if its multiple variables
        self.flatInput = True # True means that we will expect input to be a a flat vector per row, even if it smade of multiple variables

        self.lossFunctionForBuckets = nn.CrossEntropyLoss() # this is the loss function for buckets

        sample_input = sample_batch.getInput(flatten=self.flatInput)
        sample_target = sample_batch.getTarget(flatten=self.flatTarget)
        sample_target_by_buckets = sample_batch.getTarget(flatten = False, by_buckets = True)

        input_size = sample_input.size()[1]
        output_size = sample_target.size()[1]

        self.net = nn.Sequential(
            nn.Linear(input_size, input_size),
            torch.nn.LeakyReLU(),
            nn.Dropout(0.2),
            nn.Linear(input_size, int(math.ceil(input_size/2))),
            torch.nn.LeakyReLU(),
            nn.Dropout(0.2),
            nn.Linear(int(math.ceil(input_size/2)), output_size)
        )

        self.nets = { col: nn.Sequential(
            nn.Linear(output_size, sample_target_by_buckets[col].size()[1])
        ) for col in sample_target_by_buckets }

        if USE_CUDA:
            self.net.cuda()


    def calculateBatchLoss(self, batch):
        """

        :param batch:
        :return:
        """

        predicted_target, predicted_buckets = self.forward(batch.getInput(flatten=self.flatInput), return_bucket_outputs =True)
        real_target = batch.getTarget(flatten=self.flatTarget)
        real_targets_buckets = batch.getTarget(flatten=False, by_buckets=True)

        loss = 0
        for col in predicted_buckets:

            loss += self.lossFunction(predicted_buckets[col], real_targets_buckets[col])

        loss += self.lossFunction(predicted_target, real_target)
        batch_size = real_target.size()[0]
        return loss, batch_size


    def forward(self, input, return_bucket_outputs = False):
        """
        In this particular model, we just need to forward the network defined in setup, with our input

        :param input: a pytorch tensor with the input data of a batch
        :return:
        """
        output = self.net(input)

        if return_bucket_outputs is False:

            return output

        output_buckets = {}
        for col in self.nets:
            output_buckets[col] = self.nets[col](output)

        return output, output_buckets






