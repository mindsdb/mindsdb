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

        predict_column_names = self.transaction.train_metadata.model_predict_columns
        non_predict_columns = [col for col in self.transaction.persistent_model_metadata.columns if col not in predict_column_names]

        probabilistic_validators = {}

        for col in predict_column_names:
            probabilistic_validators[col] = ProbabilisticValidator(histogram=self.transaction.persistent_model_metadata.column_stats[col]['percentage_buckets'])

        # create a list of columns to ignore starting with none, and then one experiment per column
        ignore_column_options = [[]] + [[col] for col in non_predict_columns]

        # Run on the validation set multiple times, each time with one of the column blanked out
        for ignore_columns in ignore_column_options:


            validation_sampler = Sampler(self.transaction.model_data.validation_set, metadata_as_stored=self.transaction.persistent_model_metadata,
                                        ignore_types=self.transaction.data_model_object.ignore_types, sampler_mode=SAMPLER_MODES.LEARN,blank_columns=ignore_columns)
            validation_sampler.variable_wrapper = array_to_float_variable

            predictions = self.transaction.data_model_object.testModel(validation_sampler)

            # create a vector that has True for each feature that was passed to the model tester and False if it was blanked
            features_existence = [True if np_col not in ignore_columns else False for np_col in non_predict_columns]

            # A separate probabilistic model is trained for each predicted column, we may want to change this in the future, @TODO
            for pcol in predict_column_names:
                for i in range(len(predictions.predicted_targets[pcol])):

                    predicted_val = denorm(predictions.predicted_targets[pcol][i], self.transaction.persistent_model_metadata.column_stats[pcol])
                    real_val = denorm(predictions.real_targets[pcol][i], self.transaction.persistent_model_metadata.column_stats[pcol])
                    probabilistic_validators[pcol].register_observation(features_existence=features_existence, real_value=real_val, predicted_value=predicted_val)

                probabilistic_validators[pcol].partial_fit()

        # Pickle for later use
        self.transaction.persistent_model_metadata.probabilistic_validators = {}
        for col in probabilistic_validators:
            self.transaction.persistent_model_metadata.probabilistic_validators[col] = probabilistic_validators[col].pickle()

        self.transaction.persistent_model_metadata.update()

def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    #CONFIG.DEBUG_BREAK_POINT = PHASE_MODEL_ANALYZER

    #mdb = Predictor(name='home_rentals')
    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02,
        stop_training_in_x_seconds=6
    )

    # use the model to make predictions
    result = mdb.predict(
        when={'number_of_rooms': 2, 'number_of_bathrooms': 2, 'sqft': 1190})

    print(result.predicted_values)

    # use the model to make predictions
    result = mdb.predict(
        when={'number_of_rooms': 2, 'sqft': 1190})

    print(result.predicted_values)


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
