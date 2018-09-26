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

import mindsdb.config as CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.workers.predict import PredictWorker
import numpy as np
import time
import random
class ModelPredictor(BaseModule):

    phase_name = PHASE_PREDICTION

    def run(self):

        model_name = self.transaction.persistent_model_metadata.model_name
        self.train_start_time = time.time()
        self.session.logging.info('Training: model {model_name}, epoch 0'.format(model_name=model_name))

        self.last_time = time.time()

        # We moved everything to a worker so we can run many of these in parallel
        # Todo: use Ray https://github.com/ray-project/tutorial

        ret_diffs = PredictWorker.start(self.transaction.model_data, model_name=model_name)


        confusion_matrices = self.transaction.persistent_ml_model_info.confussion_matrices

        self.transaction.output_data.columns = self.transaction.input_data.columns
        # TODO: This may be inneficient, try to work on same pointer
        self.transaction.output_data.data_array = self.transaction.input_data.data_array
        self.transaction.output_data.predicted_columns=self.transaction.metadata.model_predict_columns
        for diff in ret_diffs:
            for col in diff['ret_dict']:
                confusion_matrix = confusion_matrices[col]
                col_index = self.transaction.input_data.columns.index(col)
                self.transaction.output_data.columns.insert(col_index+1,KEY_CONFIDENCE)
                offset = diff['start_pointer']
                group_pointer = diff['group_pointer']
                column_pointer = diff['column_pointer']
                for j, cell in enumerate(diff['ret_dict'][col]):
                    #TODO: This may be calculated just as j+offset
                    if not cell:
                        continue
                    actual_row = self.transaction.model_data.predict_set_map[group_pointer][j+offset]
                    if not self.transaction.output_data.data_array[actual_row][col_index] or self.transaction.output_data.data_array[actual_row][col_index] == '':

                        if self.transaction.persistent_model_metadata.column_stats[col][KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:
                            target_val = np.format_float_positional(cell, precision=2)
                        else:
                            target_val = cell
                        self.transaction.output_data.data_array[actual_row][col_index] = target_val
                        confidence = self.getConfidence(cell,confusion_matrix)
                        #randConfidence = random.uniform(0.85, 0.93)

                        self.transaction.output_data.data_array[actual_row].insert(col_index + 1, confidence)
                    else:
                        self.transaction.output_data.data_array[actual_row].insert(col_index+1,1.0)


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

    from mindsdb.libs.controllers.mindsdb_controller import MindsDBController as MindsDB
    import logging

    mdb = MindsDB()
    ret = mdb.predict(predict='position', when={'max_time_rec': 700}, model_name='mdsb_model')
    logging.info(ret)


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

