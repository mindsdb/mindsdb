

from mindsdb.config import *
from mindsdb.libs.constants.mindsdb import *

import math
import torch
import torch.nn as nn
from mindsdb.libs.ml_models.pytorch.libs.base_model import BaseModel



class FullyConnectedNet(BaseModel):


    def setup(self, sample_batch):
        """
        Here we define the basic building blocks of our model, in forward we define how we put it all together along wiht an input

        :param sample_batch: this is used to understand the characteristics of the input and target, it is an object of type utils.libs.data_types.batch.Batch
        """

        self.flatTarget = True # True means that we will expect target to be a flat vector per row, even if its multiple variables
        self.flatInput = True # True means that we will expect input to be a a flat vector per row, even if it smade of multiple variables

        sample_input = sample_batch.getInput(flatten=self.flatInput)
        sample_target = sample_batch.getTarget(flatten=self.flatTarget)
        input_size = sample_input.size()[1]
        output_size = sample_target.size()[1]

        self.net = nn.Sequential(
            nn.Linear(input_size, input_size),
            torch.nn.LeakyReLU(),
            nn.Linear(input_size, int(math.ceil(input_size/2))),
            torch.nn.LeakyReLU(),
            nn.Linear(int(math.ceil(input_size/2)), output_size)
        )



        if USE_CUDA:
            self.net.cuda()



    def forward(self, input):
        """
        In this particular model, we just need to forward the network defined in setup, with our input

        :param input: a pytorch tensor with the input data of a batch
        :return:
        """
        output = self.net(input)
        return output






