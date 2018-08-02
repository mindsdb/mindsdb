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

import config as CONFIG
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from libs.workers.train import TrainWorker

import _thread
import time



class ModelTrainer(BaseModule):

    phase_name = PHASE_MODEL_TRAINER



    def run(self):
        """
        Run the training process, we can perhaps iterate over all hyper parameters here and spun off model variations
        TODO: checkout the RISELab distributed ML projects for this

        :return: None
        """


        model_name = self.transaction.model_metadata[KEY_MODEL_NAME]
        data_models = [
            ('pytorch.models.fully_connected_net', {}),
            ('pytorch.models.ensemble_conv_net', {}),
            ('pytorch.models.ensemble_fully_connected_net', {})]

        self.train_start_time = time.time()

        self.session.logging.info('Training: model {model_name}, epoch 0'.format(model_name=model_name))

        self.last_time = time.time()
        # We moved everything to a worker so we can run many of these in parallel

        # Train column encoders


        for data_model_data in data_models:
            config = data_model_data[1]
            data_model = data_model_data[0]
            if CONFIG.EXEC_LEARN_IN_THREAD == False or len(data_models) == 1:
                TrainWorker.start(self.transaction.model_data, model_name=model_name, data_model=data_model, config=config)

            else:
                # Todo: use Ray https://github.com/ray-project/tutorial
                # Before moving to actual workers: MUST FIND A WAY TO SEND model data to the worker in an efficient way first
                _thread.start_new_thread(TrainWorker.start, (self.transaction.model_data, model_name, data_model, config))
            # return

        total_time = time.time() - self.train_start_time
        self.session.logging.info('Trained: model {model_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(model_name = model_name, total_time=total_time))





def test():

    from libs.test.test_controller import TestController

    module = TestController("CREATE MODEL FROM (SELECT * FROM vitals_tgt) AS vitals PREDICT vitals", PHASE_MODEL_TRAINER)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

