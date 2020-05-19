from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import get_value_bucket

from sklearn.naive_bayes import BernoulliNB
from sklearn.metrics import confusion_matrix
import numpy as np
import random


class ProbabilisticValidator():
    """
    # The probabilistic validator is a quick to train model used for validating the predictions of our main model
    # It is fit to the results our model gets on the validation set
    """
    _probabilistic_model = None
    _X_buff = None
    _Y_buff = None


    def __init__(self, col_stats, col_name, input_columns):
        """
        Chose the algorithm to use for the rest of the model
        As of right now we go with BernoulliNB¶
        """
        self.col_stats = col_stats
        self.col_name = col_name
        self.input_columns = input_columns

        if 'percentage_buckets' in col_stats:
            self.buckets = col_stats['percentage_buckets']

        self._probabilistic_model = BernoulliNB()

    def fit(self, real_df, predictions_arr, missing_col_arr, hmd=None):
        """
        # Fit the probabilistic validator

        :param real_df: A dataframe with the real inputs and outputs for every row
        :param predictions_arr: An array containing arrays of predictions, one containing the "normal" predictions and the rest containing predictions with various missing column
        :param missing_col_arr: The missing columns for each of the prediction arrays, same order as the arrays in `predictions_arr`, starting from the second element of `predictions_arr` (The first is assumed to have no missing columns)


        """
        self.real_values_bucketized = []
        self.normal_predictions_bucketized = []
        self.numerical_samples_arr = []

        column_indexes = {}
        for i, col in enumerate(self.input_columns):
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
            for m in range(len(real_df)):
                row = real_df.iloc[m]
                predicted_value = predictions_arr[n][self.col_name][m]

                if f'{self.col_name}_confidence_range' in predictions_arr[n]:
                    predicted_range = predictions_arr[n][f'{self.col_name}_confidence_range'][m]

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

                if self.col_stats['data_type'] == DATA_TYPES.NUMERIC:
                    Y.append(predicted_range[0] < real_value < predicted_range[1])
                else:
                    Y.append(real_value_b == predicted_value_b)

                if n == 0:
                    self.real_values_bucketized.append(real_value_b)
                    self.normal_predictions_bucketized.append(predicted_value_b)
                    if self.col_stats['data_type'] == DATA_TYPES.NUMERIC:
                        self.numerical_samples_arr.append((real_value,predicted_range))

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

        try:
            true_index = self._probabilistic_model.classes_.tolist().index(True)
        except:
            print('Only got classes: ', str(self._probabilistic_model.classes_.tolist()), ' in the probabilistic model\'s Y vector !')
            true_index = None

        if true_index is None:
            probability_true_prediction = 0
        else:
            probability_true_prediction = self._probabilistic_model.predict_proba(np.array(X))[0][true_index]

        return probability_true_prediction


    def get_accuracy_stats(self):

        bucket_accuracy = {}
        bucket_acc_counts = {}
        for i, bucket in enumerate(self.normal_predictions_bucketized):
            if bucket not in bucket_acc_counts:
                bucket_acc_counts[bucket] = []

            if len(self.numerical_samples_arr) != 0:
                bucket_acc_counts[bucket].append(self.numerical_samples_arr[i][1][0] < self.numerical_samples_arr[i][0] < self.numerical_samples_arr[i][1][1])
            else:
                bucket_acc_counts[bucket].append(1 if bucket == self.real_values_bucketized[i] else 0)

        for bucket in bucket_accuracy:
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
            'buckets': list(bucket_accuracy.keys())
            ,'accuracies': list(bucket_accuracy.values())
        }

        labels= list(set(self.real_values_bucketized))
        matrix = confusion_matrix(self.real_values_bucketized, self.normal_predictions_bucketized, labels=labels)
        matrix = [[int(y) if str(y) != 'nan' else 0 for y in x] for x in matrix]

        bucket_values = [self.buckets[i] if i < len(self.buckets) else None for i in labels]

        cm = {
            'matrix': matrix,
            'predicted': bucket_values,
            'real': bucket_values
        }

        accuracy_samples = None
        if len(self.numerical_samples_arr) > 0:
            nr_samples = min(400,len(self.numerical_samples_arr))
            sampled_numerical_samples_arr = random.sample(self.numerical_samples_arr, nr_samples)
            accuracy_samples = {
                'y': [x[0] for x in sampled_numerical_samples_arr]
                ,'x': [x[1] for x in sampled_numerical_samples_arr]
            }

        return overall_accuracy, accuracy_histogram, cm, accuracy_samples

if __name__ == "__main__":
    pass
    # Removing test for now, as tets for the new one stand-alone would require the creation of a bunch of dataframes mimicking those inputed into mindsdb and those predicted by lightwood.
