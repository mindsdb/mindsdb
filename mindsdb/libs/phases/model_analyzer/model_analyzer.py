from mindsdb.libs.helpers.general_helpers import pickle_obj, disable_console_output
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.general_helpers import evaluate_accuracy
from mindsdb.libs.helpers.probabilistic_validator import ProbabilisticValidator

import pandas as pd
import numpy as np


class ModelAnalyzer(BaseModule):
    
    def run(self):
        np.seterr(divide='warn', invalid='warn')
        """
        # Runs the model on the validation set in order to fit a probabilistic model that will evaluate the accuracy of future predictions
        """

        output_columns = self.transaction.lmd['predict_columns']
        input_columns = [col for col in self.transaction.lmd['columns'] if col not in output_columns and col not in self.transaction.lmd['columns_to_ignore']]

        # Make predictions on the validation dataset normally and with various columns missing
        normal_predictions = self.transaction.model_backend.predict('validate')
        normal_accuracy = evaluate_accuracy(normal_predictions, self.transaction.input_data.validation_df, self.transaction.lmd['column_stats'], output_columns)

        empty_input_predictions = {}
        empty_inpurt_accuracy = {}

        ignorable_input_columns = [x for x in input_columns if self.transaction.lmd['column_stats'][x]['data_type'] != DATA_TYPES.FILE_PATH and x not in [y[0] for y in self.transaction.lmd['model_order_by']]]
        for col in ignorable_input_columns:
            empty_input_predictions[col] = self.transaction.model_backend.predict('validate', ignore_columns=[col])
            empty_inpurt_accuracy[col] = evaluate_accuracy(empty_input_predictions[col], self.transaction.input_data.validation_df, self.transaction.lmd['column_stats'], output_columns)

        # Get some information about the importance of each column
        if not self.transaction.lmd['disable_optional_analysis']:
            self.transaction.lmd['column_importances'] = {}
            for col in ignorable_input_columns:
                column_importance = (1 - empty_inpurt_accuracy[col]/normal_accuracy)
                column_importance = np.ceil(10*column_importance)
                self.transaction.lmd['column_importances'][col] = float(10 if column_importance > 10 else column_importance)

        # Run Probabilistic Validator
        overall_accuracy_arr = []
        self.transaction.lmd['accuracy_histogram'] = {}
        self.transaction.lmd['confusion_matrices'] = {}
        self.transaction.lmd['accuracy_samples'] = {}
        self.transaction.hmd['probabilistic_validators'] = {}

        for col in output_columns:
            pval = ProbabilisticValidator(col_stats=self.transaction.lmd['column_stats'][col], col_name=col, input_columns=input_columns)
            predictions_arr = [normal_predictions] + [empty_input_predictions[col] for col in ignorable_input_columns]

            pval.fit(self.transaction.input_data.validation_df, predictions_arr, [[x] for x in ignorable_input_columns])
            overall_accuracy, accuracy_histogram, cm, accuracy_samples = pval.get_accuracy_stats()
            overall_accuracy_arr.append(overall_accuracy)

            self.transaction.lmd['accuracy_histogram'][col] = accuracy_histogram
            self.transaction.lmd['confusion_matrices'][col] = cm
            self.transaction.lmd['accuracy_samples'][col] = accuracy_samples
            self.transaction.hmd['probabilistic_validators'][col] = pickle_obj(pval)

        print(overall_accuracy_arr)
        self.transaction.lmd['validation_set_accuracy'] = sum(overall_accuracy_arr)/len(overall_accuracy_arr)

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
