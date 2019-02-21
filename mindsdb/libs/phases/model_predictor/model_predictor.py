from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.workers.predict import PredictWorker
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator
import numpy as np
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

        self.transaction.output_data.columns = self.transaction.input_data.columns
        # TODO: This may be inneficient, try to work on same pointer
        self.transaction.output_data.data_array = self.transaction.input_data.data_array
        self.transaction.output_data.predicted_columns=self.transaction.metadata.model_predict_columns
        self.transaction.output_data.columns_map =  self.transaction.metadata.model_columns_map

        self.transaction.probabilistic_validators = {}
        for n in range(len(ret_diffs)):
            diff = ret_diffs[n]

            for col in diff['ret_dict']:

                for col in self.transaction.persistent_model_metadata.probabilistic_validators:
                    self.transaction.probabilistic_validators[col] = ProbabilisticValidator.unpickle(self.transaction.persistent_model_metadata.probabilistic_validators[col])

                X_values = []
                X_features_existence = []
                for nn in range(len(diff['ret_dict'][col])):
                    X_features_existence.append([])

                    for col in self.transaction.session.predict_worker.predict_sampler.data['ALL_ROWS_NO_GROUP_BY']:
                        X_features_existence[nn].append(self.transaction.session.predict_worker.predict_sampler.data['ALL_ROWS_NO_GROUP_BY'][col][nn][-1])

                    denormed_predicted_val = diff['ret_dict'][col][nn]
                    X_values.append(denormed_predicted_val)

                self.accuracies[col] = []
                for i in range(len(X_values)):
                    accuracy = self.transaction.probabilistic_validators[col].evaluate_prediction_accuracy(
                    features_existence=X_features_existence[i],predicted_value=X_values[i], histogram=self.transaction.persistent_model_metadata.column_stats[col]['histogram'])
                    self.accuracies[col].append(accuracy)

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

                    actual_row = j + offset
                    # @TODO: Do we scrap this ?
                    #confidence = self.getConfidence(cell, confusion_matrix)
                    confidence = self.accuracies[col][j]
                    if self.transaction.persistent_model_metadata.column_stats[col][
                        KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:
                        target_val = np.format_float_positional(cell, precision=2)
                    else:
                        target_val = cell

                    self.transaction.output_data.data_array[actual_row].insert(col_index + 1, "{}%".format(round(100*confidence,2)))
                    self.transaction.output_data.data_array[actual_row][col_index] = target_val




        total_time = time.time() - self.train_start_time
        self.session.log.info(
            'Predict: model {model_name} [OK], TOTAL TIME: {total_time:.2f} seconds'.format(model_name=model_name,
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
        confidence = transposed[index][index]
        if confidence >=1:
            confidence = 0.99
        return "{0:.2f}".format(confidence)

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
