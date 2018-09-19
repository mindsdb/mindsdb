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

from __future__ import unicode_literals, print_function, division

import mindsdb.config as CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from collections import OrderedDict
from mindsdb.libs.workers.train import TrainWorker
from bson.objectid import ObjectId

import _thread
import time



class DataEncoder(BaseModule):

    phase_name = PHASE_DATA_ENCODER



    def run(self):
        """
        Run the training process, we can perhaps iterate over all hyper parameters here and spun off model variations
        TODO: checkout the RISELab distributed ML projects for this

        :return: None
        """


        model_name = self.transaction.model_metadata[KEY_MODEL_NAME]
        model_stats = self.session.mongo.mindsdb.model_stats
        encoders = [('pytorch.encoders.rnn', {})]

        self.train_start_time = time.time()

        self.session.logging.info('Training: model {model_name}, epoch 0'.format(model_name=model_name))

        self.last_time = time.time()

        # Train encoders for full_text columns
        for src_col in self.transaction.model_stats:
            if self.transaction.model_stats[src_col][KEYS.DATA_TYPE] == DATA_TYPES.FULL_TEXT:
                target_col = '{src_col}_target'.format(src_col=src_col)
                submodel_name = 'full_text.{src_col}'.format(src_col=src_col)
                data_model = 'pytorch.encoders.text_rnn'
                config = {}

                stats = {
                    KEY_MODEL_NAME: model_name,
                    KEY_SUBMODEL_NAME: submodel_name,
                    KEY_STATS: {src_col: self.transaction.model_stats[src_col]},
                    KEY_METADATA: {
                        KEY_MODEL_PREDICT_COLUMNS: [target_col],
                        KEY_COLUMNS: [src_col, target_col]
                    },
                    KEY_STATUS: MODEL_STATUS_TRAINING,
                    KEY_COLUMNS: [src_col, target_col], # todo: this repeats in metadata, fix it
                    "_id": str(ObjectId())
                }

                model_stats.insert(stats)

                data = {}

                for test_train_key in [KEYS.TEST_SET, KEYS.TRAIN_SET]:
                    target_group = KEY_NO_GROUP_BY
                    data[test_train_key] = {target_group:OrderedDict()}
                    data[test_train_key][target_group][src_col] = []
                    data[test_train_key][target_group][target_col] = []
                    for group in self.transaction.model_data[test_train_key]:
                        data[test_train_key][target_group][src_col]+=self.transaction.model_data[test_train_key][group][src_col]
                    data[test_train_key][target_group][target_col] = data[test_train_key][target_group][src_col]

                self.session.logging.info(
                    'Training: model {model_name}, submodel {submodel_name}'.format(
                        model_name=model_name, total_time=total_time, submodel_name=submodel_name))

                train_start_time = time.time()
                TrainWorker.start(data, model_name=model_name, ml_model=data_model,
                                  config=config, submodel_name=submodel_name)
                total_time = time.time() - train_start_time
                self.session.logging.info(
                    'Trained: model {model_name}, submodel {submodel_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(
                        model_name=model_name, total_time=total_time, submodel_name=submodel_name))

        self.session.logging.info('Trained: model {model_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(model_name = model_name, total_time=total_time))





def test():

    from mindsdb.libs.test.test_controller import TestController

    module = TestController("CREATE MODEL FROM (SELECT * FROM Uploads.views.tweets2) AS tweets2 PREDICT retweet_count", PHASE_DATA_ENCODER)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

