from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.data_types.probability_evaluation import ProbabilityEvaluation
from mindsdb.libs.helpers.general_helpers import get_value_bucket

from sklearn.naive_bayes import BernoulliNB
from sklearn.metrics import confusion_matrix
import numpy as np


class ProbabilisticValidator():
    """
    # The probabilistic validator is a quick to train model used for validating the predictions of our main model
    # It is fit to the results our model gets on the validation set
    """
    _probabilistic_model = None
    _X_buff = None
    _Y_buff = None


    def __init__(self, col_stats, col_name, input_columns, data_type=None):
        """
        Chose the algorithm to use for the rest of the model
        As of right now we go with BernoulliNB¶
        """
        self._X_buff = []
        self._Y_buff = []
        self._predicted_buckets_buff = []
        self._real_buckets_buff = []
        self._original_real_buckets_buff = []
        self._original_predicted_buckets_buff = []

        
        self.real_values_bucketized = []
        self.normal_predictions_bucketized = []
        self.col_stats = col_stats
        self.col_name = col_name
        self.input_columns = input_columns

        if 'percentage_buckets' in col_stats:
            self.buckets = col_stats['percentage_buckets']

        self._probabilistic_model = BernoulliNB()

        self.bucket_accuracy = {}

    def fit(self, real_df, predictions_arr, missing_col_arr, hmd=None):
        """
        # Fit the probabilistic validator

        :param real_df: A dataframe with the real inputs and outputs for every row
        :param predictions_arr: An array containing arrays of predictions, one containing the "normal" predictions and the rest containing predictions with various missing column
        :param missing_col_arr: The missing columns for each of the prediction arrays, same order as the arrays in `predictions_arr`, starting from the second element of `predictions_arr` (The first is assumed to have no missing columns)


        """

        column_indexes = {}
        for i, col in self.input_columns:
            column_indexes[col] = i

        real_present_inputs_arr = []
        for _, row in real_df.iterrows():
            present_inputs = [1] * len(self.input_columns)
            for i, col in enumerate(self.input_columns):
                if str(row[col]) in ('None', 'nan', '', 'Nan', 'NAN', 'NaN'):
                    present_inputs[i] = 0
            real_present_inputs_arr.append(present_inputs)

        X = []
        Y = []
        for n in range(len(predictions_arr)):
            for m, row in real_df.iterrows():
                predicted_value = predictions_arr[n][m]
                real_value = row[self.col_name]
                try:
                    predicted_value = predicted_value if self.col_stats['data_type'] != DATA_TYPES.NUMERIC else float(predicted_value)
                except:
                    predicted_value = None

                try:
                    real_value = real_value if self.col_stats['data_type'] != DATA_TYPES.NUMERIC else float(str(real_value).replace(',','.'))
                except:
                    real_value = None

                if self.buckets is not None: 
                    predicted_value_b = get_value_bucket(predicted_value, self.buckets, self.col_stats, hmd)
                    real_value_b = get_value_bucket(real_value, self.buckets, self.col_stats, hmd)

                    X.append([0] * (len(self.buckets) + 1))
                    X[-1][predicted_value_b] = 1

                    
                else:
                    predicted_value_b = predicted_value
                    real_value_b = real_value_b
                
                    X.append([])
                
                Y.append(real_value_b == predicted_value_b)

                if n == 0:
                    self.real_values_bucketized.append(real_value_b)
                    self.normal_predictions_bucketized.append(predicted_value_b)

                feature_existance = real_present_inputs_arr[m]
                if n > 0:
                    for missing_col in missing_col_arr[n - 1]:
                        feature_existance[self.input_columns.index(missing_col)] = 0

                X[-1] += feature_existance
                

        log_types = np.seterr()
        np.seterr(divide='ignore')
        self._probabilistic_model.fit(X, Y)
        np.seterr(divide=log_types['divide'])


    def evaluate_prediction_accuracy(self, features_existence, predicted_value):
        """
        # Fit the probabilistic validator on an observation
        :param features_existence: A vector of 0 and 1 representing the existence of all the features (0 == not exists, 1 == exists)
        :param predicted_value: The predicted value/label
        :return: The probability (from 0 to 1) of our prediction being accurate (within the same histogram bucket as the real value)
        """
        if self.buckets is not None:
            predicted_value_b = get_value_bucket(predicted_value, self.buckets, self.col_stats)
            X = [0] * (len(self.buckets) + 1)
            X[predicted_value_b] = 1
            X = [X + features_existence]
        else:
            X = [features_existence]

        probability_true_prediction = self._probabilistic_model.predict_proba(np.array(X))[0][self._probabilistic_model.classes_.tolist().index(True)]
        return probability_true_prediction


    def get_accuracy_stats(self):
        x = []
        y = []

        total_correct = 0
        total_vals = 0

        self.real_values_bucketized
        self.normal_predictions_bucketized

        bucket_accuracy = {}
        bucket_acc_counts = {}
        for i, bucket in enumerate(self.normal_predictions_bucketized):
            bucket_acc_counts[bucket].append(1 if bucket == self.real_values_bucketized[i] else 0)
        
        for bucket in self.bucket_accuracy:
            bucket_accuracy[bucket] = sum(bucket_acc_counts[bucket])/len(bucket_acc_counts[bucket])

        accuracy_count = []
        for counts in list(bucket_acc_counts.values()):
            accuracy_count += counts
        overall_accuracy = sum(accuracy_count)/len(accuracy_count)

        for bucket in range(len(self.buckets)):
            if bucket not in bucket_accuracy:
                if bucket in self.real_values_bucketized:
                    # If it was never predicted, but it did exist as a real value, then assume 0% confidence when it does get predicted
                    bucket_accuracy[bucket] = 0

        for bucket in range(len(self.buckets)):
            if bucket not in bucket_accuracy:
                # If it wasn't seen either in the real values or in the predicted values, assume average confidence (maybe should be 0 instead ?)
                bucket_accuracy[bucket] = overall_accuracy

        accuracy_histogram = {
            'buckets': list(self.bucket_accuracy.keys())
            ,'accuracies': list(self.bucket_accuracy.keys())
        }

        labels= list(set(self.real_values_bucketized))
        matrix = confusion_matrix(self.real_values_bucketized, self.normal_predictions_bucketized, labels=labels)

        value_labels = []
        for label in labels:
            try:
                value_labels.append(str(self.buckets[label]))
            except:
                value_labels.append('UNKNOWN')

        matrix = [[int(y) if str(y) != 'nan' else 0 for y in x] for x in matrix]

        confusion_matrix_obj = {
            'matrix': matrix,
            'predicted': value_labels,
            'real': value_labels
        }
        return confusion_matrix_obj
        
        return overall_accuracy, accuracy_histogram, cm 

    def get_confusion_matrix(self):
        # The rows represent predicted values
        # The "columns" represent real values
        labels= list(set(self._original_real_buckets_buff))

        matrix = confusion_matrix(self._original_real_buckets_buff, self._original_predicted_buckets_buff, labels=labels)

        value_labels = []
        for label in labels:
            try:
                value_labels.append(str(self.buckets[label]))
            except:
                value_labels.append('UNKNOWN')

        matrix = [[int(y) if str(y) != 'nan' else 0 for y in x] for x in matrix]

        confusion_matrix_obj = {
            'matrix': matrix,
            'predicted': value_labels,
            'real': value_labels
        }
        return confusion_matrix_obj


if __name__ == "__main__":
    import random

    values = [2,2,2,3,5,2,2,2,3,5]
    predictions = [2,2,2,3,2,2,2,2,3,2]

    feature_rows = [
        [bool(random.getrandbits(1)), bool(random.getrandbits(1)), bool(random.getrandbits(1))]
        for i in values
    ]

    pbv = ProbabilisticValidator(col_stats={'percentage_buckets':[1,2,3,4,5], 'data_type':DATA_TYPES.NUMERIC, 'data_subtype': ''})

    for i in range(len(feature_rows)):
        pbv.register_observation(feature_rows[i],values[i], predictions[i])

    pbv.partial_fit()
    print(pbv.evaluate_prediction_accuracy([True,True,True], 2))

    # Now test text tokens
    values = ['2', '2', '2', '3', '5', '2', '2', '2', '3', '5']
    predictions = ['2', '2', '2', '3', '2', '2', '2', '2', '3', '2']

    feature_rows = [
        [bool(random.getrandbits(1)), bool(random.getrandbits(1)), bool(random.getrandbits(1))]
        for i in values
    ]

    print(feature_rows)

    pbv = ProbabilisticValidator(col_stats={'percentage_buckets':[1,2,3,4,5], 'data_type':DATA_TYPES.CATEGORICAL, 'data_subtype': ''})

    for i in range(len(feature_rows)):
        pbv.register_observation(feature_rows[i], values[i], predictions[i])

    pbv.partial_fit()
    print(pbv.evaluate_prediction_accuracy([True, True, True], '2'))
