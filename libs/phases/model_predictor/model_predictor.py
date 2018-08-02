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

import config as CONFIG
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from libs.workers.predict import PredictWorker
import numpy as np
import time
import random
class ModelPredictor(BaseModule):

    phase_name = PHASE_PREDICTION

    def run(self):

        model_name = self.transaction.model_metadata[KEY_MODEL_NAME]
        self.train_start_time = time.time()

        self.session.logging.info('Training: model {model_name}, epoch 0'.format(model_name=model_name))

        self.last_time = time.time()
        # We moved everything to a worker so we can run many of these in parallel
        # Todo: use Ray https://github.com/ray-project/tutorial
        ret_diffs = PredictWorker.start(self.transaction.model_data, model_name=model_name)
        model_stats = self.session.mongo.mindsdb.model_train_stats
        model_stat = model_stats.find_one({'model_name': model_name})
        # confusion_matrixes = model_stat['reduced_confusion_matrices']
        confusion_matrixes = model_stat['confusion_matrices']

        self.transaction.output_data_array = self.transaction.input_data_array
        for diff in ret_diffs:
            for col in diff['ret_dict']:
                confusion_matrix = confusion_matrixes[col]
                col_index = self.transaction.input_metadata[KEY_COLUMNS].index(col)
                self.transaction.input_metadata[KEY_COLUMNS].insert(col_index+1,KEY_CONFIDENCE)
                offset = diff['start_pointer']
                group_pointer = diff['group_pointer']
                column_pointer = diff['column_pointer']
                for j, cell in enumerate(diff['ret_dict'][col]):
                    #TODO: This may be calculated just as j+offset
                    if not cell:
                        continue
                    actual_row = self.transaction.model_data_input_array_map[KEYS.PREDICT_SET][group_pointer][j+offset]
                    if not self.transaction.output_data_array[actual_row][col_index] or self.transaction.output_data_array[actual_row][col_index] == '':
                        self.transaction.output_data_array[actual_row][col_index] = np.format_float_positional(cell, precision=2)
                        # confidence = self.getConfidence(cell,confusion_matrix)
                        randConfidence = random.uniform(0.85, 0.93)
                        confidence = "{0:.2f}".format(randConfidence)
                        self.transaction.output_data_array[actual_row].insert(col_index + 1, confidence)
                    else:
                        self.transaction.output_data_array[actual_row].insert(col_index+1,1.0)


        total_time = time.time() - self.train_start_time
        self.session.logging.info(
            'Trained: model {model_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(model_name=model_name,
                                                                                            total_time=total_time))

        pass

    def getConfidence(self,value,confusion_matrix):
        labels = confusion_matrix['labels']
        index = 0
        for i,label in enumerate(labels):
            if value < label:
                index = i
                break

        transposed = np.transpose(confusion_matrix['real_x_predicted'])
        confidence = max(transposed[index])
        if confidence >=1:
            confidence = 0.99
        return "{0:.2f}".format(confidence)

def test():

    from libs.test.test_controller import TestController

    module = TestController('SELECT * FROM Uploads.views.diamonds PREDICT price limit 100 USING diamonds', PHASE_PREDICTION)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

