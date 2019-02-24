from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator
from mindsdb.libs.ml_models.pytorch.libs.torch_helpers import array_to_float_variable
from mindsdb.libs.helpers.norm_denorm_helpers import denorm

import pandas as pd

class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        """
        # Runs the model on the validation set in order to fit a probabilistic model that will evaluate the accuracy of future predictions
        """

        column_names = self.transaction.model_data.validation_set['ALL_ROWS_NO_GROUP_BY'].keys()
        probabilistic_validators = {}

        for col in column_names:
            probabilistic_validators[col] = ProbabilisticValidator()

        # Run on the validation set multiple times, each time with one of the column blanked out
        for column_name in column_names:
            ignore_columns = []
            if column_name not in self.transaction.train_metadata.model_predict_columns:
                ignore_columns.append(column_name)

            validation_sampler = Sampler(self.transaction.model_data.validation_set, metadata_as_stored=self.transaction.persistent_model_metadata,
                                        ignore_types=self.transaction.data_model_object.ignore_types, sampler_mode=SAMPLER_MODES.LEARN,blank_columns=ignore_columns)
            validation_sampler.variable_wrapper = array_to_float_variable

            predictions = self.transaction.data_model_object.testModel(validation_sampler)

            # A separate probabilistic model is trained for each predicted column, we may want to change this in the future, @TODO
            for pcol in predictions.predicted_targets:
                for i in range(len(predictions.predicted_targets[pcol])):
                    features_existence = []
                    for col in column_names:
                        if col in ignore_columns:
                            features_existence.append(0)
                        else:
                            if len(validation_sampler.data['ALL_ROWS_NO_GROUP_BY'][col][i]) > 2:
                                if len([x for x in validation_sampler.data['ALL_ROWS_NO_GROUP_BY'][col][i] if x > 0]) == 0:
                                    features_existence.append(0)
                                else:
                                    features_existence.append(1)
                            else:
                                features_existence.append(validation_sampler.data['ALL_ROWS_NO_GROUP_BY'][col][i][-1])

                    predicted_val = denorm(predictions.predicted_targets[pcol][i], self.transaction.persistent_model_metadata.column_stats[pcol])
                    real_val = denorm(predictions.real_targets[pcol][i], self.transaction.persistent_model_metadata.column_stats[pcol])

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
