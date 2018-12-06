from mindsdb.config import *
from mindsdb.libs.constants.mindsdb import *

import torch
import math
import torch.nn as nn
import numpy as np

from mindsdb.libs.ml_models.pytorch.libs.base_model import BaseModel
from mindsdb.libs.ml_models.pytorch.encoders.rnn.encoder_rnn import EncoderRNN


class EnsembleFullyConnectedNet(BaseModel):

    ignore_types = [] # NONE
    use_full_text_input = True

    def setup(self, sample_batch):
        """
        Here we define the basic building blocks of our model, in forward we define how we put it all together along wiht an input

        :param sample_batch: this is used to understand the characteristics of the input and target, it is an object of type utils.libs.data_types.batch.Batch
        """

        self.flatTarget = True # True means that we will expect target to be a flat vector per row, even if its multiple variables
        self.flatInput = False # True means that we will expect input to be a dictionary of flat vectors per column
        self.fulltext_encoder_hidden_size = 128 # this is the size of the vector we encode text, TODO: This can be a config value
        self.input_encoder_size = 3 # this is what we encode each input column to. TODO: This can be a config value
        self.dropout_pct = 0.2 # this is what we dropout of the neurons to minimize saddle points TODO: This can be a config value

        sample_input = sample_batch.getInput(flatten=self.flatInput)
        sample_full_text_input = sample_batch.getFullTextInput()
        sample_target = sample_batch.getTarget(flatten=self.flatTarget)
        output_size = sample_target.size()[1]

        # we have one neural network column and encoders for the ones that are full_text
        # so
        # if not full_text:
        #   col -> net -> out
        # else:
        #   col -> encoder -> net -> out

        self.nets = {}
        self.encoders = {}


        input_cols = [col for col in sample_input] + [col for col in sample_full_text_input]

        for col in input_cols:

            is_text = True if sample_batch.sampler.stats[col][KEYS.DATA_TYPE] == DATA_TYPES.FULL_TEXT else False

            is_numeric_or_date = True if sample_batch.sampler.stats[col][KEYS.DATA_TYPE] in [DATA_TYPES.NUMERIC, DATA_TYPES.DATE] else False

            if is_text: # also create an encoder
                lang_len = len(sample_batch.sampler.stats[col]['dictionary']) + FULL_TEXT_ENCODING_EXTRA_LENGTH
                self.encoders[col] = EncoderRNN(lang_len, self.fulltext_encoder_hidden_size)
                initial_size = self.fulltext_encoder_hidden_size
            else:
                initial_size = sample_input[col].size()[1]

            if initial_size > self.input_encoder_size*2:

                self.nets[col] =   nn.Sequential(
                    nn.Linear(initial_size, self.input_encoder_size*2),
                    torch.nn.LeakyReLU(),
                    nn.Dropout(self.dropout_pct),
                    nn.Linear(self.input_encoder_size*2, self.input_encoder_size),
                    torch.nn.LeakyReLU(),
                    nn.Dropout(self.dropout_pct),
                    nn.Linear(self.input_encoder_size, output_size)
                )

            else:

                self.nets[col] = nn.Sequential(
                    nn.Linear(sample_input[col].size()[1], output_size),
                )



        self.ordered_cols = [col for col in self.nets]


        # the input of the last neural net is the flat input + all the predictions per column
        input2_size = (len(sample_input) + len(sample_full_text_input))*output_size #+ sum([sample_input[col].size()[1] for col in sample_input])


        self.regnet = nn.Sequential(
            nn.Linear(input2_size, int(math.ceil(input2_size/2))),
            torch.nn.LeakyReLU(),
            nn.Dropout(self.dropout_pct),
            nn.Linear(int(math.ceil(input2_size/2)), output_size),
            torch.nn.LeakyReLU()
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

        predicted_target, inner_preds = self.forward(batch.getInput(flatten=self.flatInput), full_text_input = batch.getFullTextInput(), return_inner_outputs =True)
        real_target = batch.getTarget(flatten=self.flatTarget)
        loss = 0
        for pred in inner_preds:
            loss += self.lossFunction(pred, real_target)

        loss = loss/len(inner_preds)

        loss += self.lossFunction(predicted_target, real_target)
        batch_size = real_target.size()[0]
        return loss, batch_size



    def forward(self, input, full_text_input, return_inner_outputs = False):
        """
        In this particular model, we just need to forward the network defined in setup, with our input

        :param input: a pytorch tensor with the input data of a batch
        :return:
        """
        inner_outputs = []

        for col in self.ordered_cols:

            if col in self.encoders and col in full_text_input:
                encoder_hidden = self.encoders[col].initHidden()
                total_rows = len(full_text_input[col])  # this should give the number of rows, TODO: Confirm this
                input_tensor = [] # TODO:this should be an empty tensor

                # encode each row
                for row_i in range(total_rows):
                    # foreach row encode each word, take the encoder hidden
                    r_input_length = len(full_text_input[col][row_i])

                    for ei in range(r_input_length):
                        encoder_output, encoder_hidden = self.encoders[col](full_text_input[col][row_i][ei], encoder_hidden)

                    # use the last encoded hidden states to create an input vector for each row
                    input_tensor += [encoder_hidden] # todo this is more about appending rows to the tensor

                # run the column network with the hidden estates
                inner_outputs += [self.nets[col](torch.cat(input_tensor,1)[0])]

            else:
                inner_outputs += [self.nets[col](input[col])]


        base_outputs = tuple(inner_outputs)
        input_tensor = torch.cat(base_outputs,1)
        output = self.regnet(input_tensor)

        if return_inner_outputs == True:
            return output, inner_outputs
        else:
            return output






