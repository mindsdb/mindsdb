from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.model_examination.probabilistic_validator import ProbabilisticValidator, HypothesisExecutor

import pandas as pd
import numpy as np

class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        """
        # Runs the model on the validation set in order to fit a probabilistic model that will evaluate the accuracy of future predictions
        """

        output_columns = output_columns
        input_columns = [col if col not in output_columns for col in self.transaction.persistent_model_metadata.column]

        # Test some hypotheses about our columns
        hypothesis_executor = HypothesisExecutor()
        column_importance = hypothesis_executor.run(model=self.transaction.model_backend, output_columns=output_columns, input_columns=input_columns,
        full_dataset=self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY], stats=self.transaction.persistent_model_metadata.column_stats)

        exit()

        # Create the probabilistic validators for each of the predict column
        probabilistic_validators = {}
        for col in output_columns:
            if 'percentage_buckets' in self.transaction.persistent_model_metadata.column_stats[col]:
                probabilistic_validators[col] = ProbabilisticValidator(
                    buckets=self.transaction.persistent_model_metadata.column_stats[col]['percentage_buckets'], data_type=self.transaction.persistent_model_metadata.column_stats[col]['data_type'])
            else:
                probabilistic_validators[col] = ProbabilisticValidator(
                    buckets=None, data_type=self.transaction.persistent_model_metadata.column_stats[col]['data_type'])

        # Run on the validation set multiple times, each time with one of the column blanked out
        for column_name in input_columns:
            ignore_columns = []
            ignore_columns.append(column_name)

            predictions = self.transaction.model_backend.predict('validate', ignore_columns)

            # create a vector that has True for each feature that was passed to the model tester and False if it was blanked
            features_existence = [True if np_col not in ignore_columns else False for np_col in input_columns]

            # A separate probabilistic model is trained for each predicted column, we may want to change this in the future, @TODO
            for pcol in output_columns:
                for i in range(len(predictions[pcol])):
                    predicted_val = predictions[pcol][i]
                    real_val = self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY][pcol][i]
                    probabilistic_validators[pcol].register_observation(features_existence=features_existence, real_value=real_val, predicted_value=predicted_val)


        for pcol in output_columns:
            probabilistic_validators[pcol].partial_fit()

        # Pickle for later use
        self.transaction.persistent_model_metadata.probabilistic_validators = {}
        for col in probabilistic_validators:
            self.transaction.persistent_model_metadata.probabilistic_validators[col] = probabilistic_validators[col].pickle()

        self.transaction.persistent_model_metadata.update()

def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        #sample_margin_of_error=0.02,
        stop_training_in_x_seconds=6
    )

    #use the model to make predictions
    result = mdb.predict(
        when={"number_of_rooms": 2, "sqft": 1384})

    result[0].explain()

    when = {"number_of_rooms": 1,"sqft": 384}

    # use the model to make predictions
    result = mdb.predict(
        when=when)

    result[0].explain()


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
