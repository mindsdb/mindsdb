from mindsdb.libs.helpers.general_helpers import pickle_obj
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.probabilistic_validator import ProbabilisticValidator
from mindsdb.libs.phases.model_analyzer.helpers.column_evaluator import ColumnEvaluator

import pandas as pd
import numpy as np

class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        """
        # Runs the model on the validation set in order to fit a probabilistic model that will evaluate the accuracy of future predictions
        """

        output_columns = self.transaction.lmd['predict_columns']
        input_columns = [col for col in self.transaction.lmd['columns'] if col not in output_columns and col not in self.transaction.lmd['malformed_columns']['names']]

        # Test some hypotheses about our columns

        if self.transaction.lmd['disable_optional_analysis'] is False:
            column_evaluator = ColumnEvaluator(self.transaction)
            column_importances, buckets_stats, columnless_prediction_distribution, all_columns_prediction_distribution = column_evaluator.get_column_importance(model=self.transaction.model_backend, output_columns=output_columns, input_columns=input_columns, full_dataset=self.transaction.input_data.validation_df, stats=self.transaction.lmd['column_stats'])

            self.transaction.lmd['column_importances'] = column_importances
            self.transaction.lmd['columns_buckets_importances'] = buckets_stats
            self.transaction.lmd['columnless_prediction_distribution'] = columnless_prediction_distribution
            self.transaction.lmd['all_columns_prediction_distribution'] = all_columns_prediction_distribution

        # Create the probabilistic validators for each of the predict column
        probabilistic_validators = {}
        for col in output_columns:
            if 'percentage_buckets' in self.transaction.lmd['column_stats'][col]:
                probabilistic_validators[col] = ProbabilisticValidator(
                    col_stats=self.transaction.lmd['column_stats'][col])
            else:
                probabilistic_validators[col] = ProbabilisticValidator(
                    col_stats=self.transaction.lmd['column_stats'][col])

        ignorable_input_columns = []
        for input_column in input_columns:
            if self.transaction.lmd['column_stats'][input_column]['data_type'] != DATA_TYPES.FILE_PATH:
                ignorable_input_columns.append(input_column)


        normal_predictions = self.transaction.model_backend.predict('validate')

        # Single observation on the validation dataset when we have no ignorable column
        if len(ignorable_input_columns) == 0:
            for pcol in output_columns:
                for i in range(len(self.transaction.input_data.validation_df[pcol])):
                    probabilistic_validators[pcol].register_observation(features_existence=[True for col in input_columns], real_value=self.transaction.input_data.validation_df[pcol].iloc[i], predicted_value=normal_predictions[pcol][i])

        # Run on the validation set multiple times, each time with one of the column blanked out
        for column_name in ignorable_input_columns:
            ignore_columns = []
            ignore_columns.append(column_name)

            ignore_col_predictions = self.transaction.model_backend.predict('validate', ignore_columns)

            # create a vector that has True for each feature that was passed to the model tester and False if it was blanked
            features_existence = [True if np_col not in ignore_columns else False for np_col in input_columns]

            # A separate probabilistic model is trained for each predicted column, we may want to change this in the future, @TODO
            for pcol in output_columns:
                for i in range(len(self.transaction.input_data.validation_df[pcol])):
                    probabilistic_validators[pcol].register_observation(features_existence=features_existence, real_value=self.transaction.input_data.validation_df[pcol].iloc[i], predicted_value=ignore_col_predictions[pcol][i])
                    probabilistic_validators[pcol].register_observation(features_existence=[True for col in input_columns], real_value=self.transaction.input_data.validation_df[pcol].iloc[i], predicted_value=normal_predictions[pcol][i])

        self.transaction.lmd['accuracy_histogram'] = {}

        total_accuracy = 0
        for pcol in output_columns:
            probabilistic_validators[pcol].partial_fit()
            accuracy_histogram, validation_set_accuracy = probabilistic_validators[pcol].get_accuracy_histogram()
            self.transaction.lmd['accuracy_histogram'][pcol] = accuracy_histogram
            total_accuracy += validation_set_accuracy
        self.transaction.lmd['validation_set_accuracy'] = total_accuracy/len(output_columns)

        # Pickle for later use
        self.transaction.hmd['probabilistic_validators'] = {}
        for col in probabilistic_validators:
            self.transaction.hmd['probabilistic_validators'][col] = pickle_obj(probabilistic_validators[col])

def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
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
