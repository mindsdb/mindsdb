from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator
from mindsdb.libs.ml_models.pytorch.libs.torch_helpers import array_to_float_variable

import pandas as pd
import numpy as np

class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        """
        # Runs the model on the validation set in order to fit a probabilistic model that will evaluate the accuracy of future predictions
        """

        probabilistic_validators = {}


        ### Create the real values for the created columns (maybe move to a new 'validate' method of the mode backend ?)
        validation_data = {}

        indexes = self.transaction.input_data.validation_indexes['ALL_ROWS_NO_GROUP_BY']
        for col_ind, col in enumerate(self.transaction.persistent_model_metadata.columns):
            validation_data[col] = []
            for row_ind in indexes:
                validation_data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])
        ###

        for column_name in self.transaction.persistent_model_metadata.predict_columns:
            probabilistic_validators[column_name] = ProbabilisticValidator()
            
        # Run on the validation set multiple times, each time with one of the column blanked out
        for column_name in self.transaction.persistent_model_metadata.predict_columns:
            ignore_columns = []
            if column_name not in self.transaction.persistent_model_metadata.predict_columns:
                ignore_columns.append(column_name)

            predictions = self.transaction.model_backend.predict('validate',ignore_columns)

            # A separate probabilistic model is trained for each predicted column, we may want to change this in the future, @TODO
            for pcol in predictions:
                for i in range(len(predictions[pcol])):
                    features_existence = []
                    for col in self.transaction.persistent_model_metadata.columns:
                        if col in self.transaction.persistent_model_metadata.predict_columns:
                            continue
                        elif col in ignore_columns:
                            features_existence.append(0)
                        else:
                            if str(validation_data[col][i]) in [str(''), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null']:
                                features_existence.append(0)
                            else:
                                features_existence.append(1)

                    predicted_val = predictions[pcol][i]
                    real_val = validation_data[pcol][i]

                    probabilistic_validators[pcol].register_observation(features_existence=features_existence,
                    real_value=real_val, predicted_value=predicted_val, histogram=self.transaction.persistent_model_metadata.column_stats[pcol]['histogram'])
                probabilistic_validators[pcol].partial_fit()

        # Pickle for later use
        self.transaction.persistent_model_metadata.probabilistic_validators = {}
        for col in probabilistic_validators:
            self.transaction.persistent_model_metadata.probabilistic_validators[col] = probabilistic_validators[col].pickle()

        self.transaction.persistent_model_metadata.update()

def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    CONFIG.DEBUG_BREAK_POINT = PHASE_MODEL_ANALYZER

    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02,
        stop_training_in_x_seconds=3
    )



# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
