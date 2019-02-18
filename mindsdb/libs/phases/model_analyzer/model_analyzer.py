from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.ml_models.pytorch.libs import base_model;
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator

import pandas as pd

class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):

        '''
        @ <--- field ids is not yet set at this point
        bm = base_model.BaseModel(validation_sampler.getSampleBatch())
        self.data_model_object = bm.load_from_disk(file_ids=self.transaction.persistent_ml_model_info.fs_file_ids)
        '''

        probabilistic_validator = ProbabilisticValidator()

        input = self.transaction.model_data.validation_set['ALL_ROWS_NO_GROUP_BY']


        real_values = []
        for col in self.transaction.metadata.model_predict_columns:
            real_values.append(input[col])
            input.pop(col, None)

        # <--- Remove the pop and use bellow line if this is a pd dataframe instead
        #features = input.drop(self.transaction.metadata.model_predict_columns, axis=1)
        features = input

        predictions = self.transaction.data_model_object.forward(features)
        for col in predictions:
            for i in range(predictions[col]):
                predicted = predictions[col][i]
                real = real_values[i]
                features = features_arr[i]
                register_observation(features, real, predicted)
