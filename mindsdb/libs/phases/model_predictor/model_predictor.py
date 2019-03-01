from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.workers.predict import PredictWorker
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator
import numpy as np
import pandas as pd
import time


class ModelPredictor(BaseModule):

    phase_name = PHASE_PREDICTION

    def run(self):

        model_name = self.transaction.persistent_model_metadata.model_name
        self.train_start_time = time.time()
        self.session.log.info('Predict: model {model_name}, epoch 0'.format(model_name=model_name))

        self.last_time = time.time()
        self.accuracies = {}
        # We moved everything to a worker so we can run many of these in parallel
        # Todo: use Ray https://github.com/ray-project/tutorial

        # cache object
        if self.transaction.session.predict_worker is None:
            self.transaction.session.predict_worker = PredictWorker.get_worker_object(model_name=model_name)

        ret_diffs = self.transaction.session.predict_worker.predict(data=self.transaction.model_data)


        confusion_matrices = self.transaction.persistent_ml_model_info.confussion_matrices




        # This is to deal with the predicted values and the extra info more accurately
        columns = self.transaction.input_data.columns
        columns_to_predict = self.transaction.persistent_model_metadata.predict_columns
        input_columns = [col for col in columns if col not in columns_to_predict]

        # transform input data into ordered by columns structure
        self.transaction.output_data.data = {col: [] for i, col in enumerate(columns)}
        output_data = self.transaction.output_data.data # pointer

        for row in self.transaction.input_data.data_array:
            for index, cell in enumerate(row):
                col = columns[index]
                output_data[col].append(cell)


        # populate predictions in output_data
        for n in range(len(ret_diffs)):
            diff = ret_diffs[n]
            for col in diff['ret_dict']:

                offset = diff['start_pointer']

                for j, cell in enumerate(diff['ret_dict'][col]):

                    if not cell:
                        continue

                    actual_row = j + offset

                    if self.transaction.persistent_model_metadata.column_stats[col][
                        KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:
                        target_val = np.format_float_positional(cell, precision=2)
                    else:
                        target_val = cell

                    output_data[col][actual_row] = target_val

        # now populate the most likely value and confidence info
        probabilistic_validators = {}

        self.transaction.output_data.evaluations = {col:[None]*len(output_data[col]) for col in columns_to_predict}
        output_data_evaluations = self.transaction.output_data.evaluations

        for col in columns_to_predict:
            probabilistic_validators[col] = ProbabilisticValidator.unpickle(self.transaction.persistent_model_metadata.probabilistic_validators[col])
            confidence_column_name = "_{col}_confidence".format(col=col)
            output_data[confidence_column_name] = [None]*len(output_data[col])

            for row_number, predicted_value in enumerate(output_data[col]):

                features_existance_vector = [False if output_data[col][row_number] is None else True for col in input_columns]
                prediction_evaluation = probabilistic_validators[col].evaluate_prediction_accuracy(features_existence=features_existance_vector, predicted_value=predicted_value)
                prediction_evaluation.logger = self.log
                # replace predicted value, with most likely value for this prediction
                output_data[col][row_number] = prediction_evaluation.most_likely_value
                output_data[confidence_column_name][row_number] = prediction_evaluation.most_likely_probability
                output_data_evaluations[col][row_number] = prediction_evaluation


        # since output_data_evaluations and output_data are pointers we dont need to update self.transaction.output_data




        total_time = time.time() - self.train_start_time
        self.session.log.info(
            'Predict: model {model_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(model_name=model_name,
                                                                                            total_time=total_time))


def test():
    from mindsdb.libs.controllers.predictor import Predictor


    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02
    )

    mdb = Predictor(name='home_rentals')

    a = mdb.predict(when={'number_of_rooms': 10})

    print('-------Preidiction output------------')
    print(a.predicted_values)


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
