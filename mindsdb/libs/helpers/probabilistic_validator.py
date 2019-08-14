from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.data_types.probability_evaluation import ProbabilityEvaluation
from mindsdb.libs.helpers.general_helpers import get_value_bucket

from sklearn.naive_bayes import GaussianNB, ComplementNB, MultinomialNB
import numpy as np


class ProbabilisticValidator():
    """
    # The probabilistic validator is a quick to train model used for validating the predictions of our main model
    # It is fit to the results our model gets on the validation set
    """
    _smoothing_factor = 0.5 # TODO: Autodetermine smotthing factor depending on the info we know about the dataset
    _probabilistic_model = None
    _X_buff = None
    _Y_buff = None


    def __init__(self, col_stats, data_type=None):
        """
        Chose the algorithm to use for the rest of the model
        As of right now we go with ComplementNB
        """
        self._X_buff = []
        self._Y_buff = []

        self.col_stats = col_stats

        if 'percentage_buckets' in col_stats:
            self._probabilistic_model = MultinomialNB(alpha=self._smoothing_factor)

            self.buckets = col_stats['percentage_buckets']
            self.bucket_keys = [i for i in range(len(self.buckets))]
        else:
            self._probabilistic_model = ComplementNB(alpha=self._smoothing_factor)

            self.buckets = None

        self.data_type = col_stats['data_type']

        self.bucket_accuracy = {}


    def register_observation(self, features_existence, real_value, predicted_value, hmd=None):
        """
        # Register an observation in the validator's internal buffers

        :param features_existence: A vector of 0 and 1 representing the existence of all the features (0 == not exists, 1 == exists)
        :param real_value: The real value/label for this prediction
        :param predicted_value: The predicted value/label
        :param histogram: The histogram for the predicted column, which allows us to bucketize the `predicted_value` and `real_value`
        """

        predicted_value = predicted_value if self.data_type != DATA_TYPES.NUMERIC else float(predicted_value)
        try:
            real_value = real_value if self.data_type != DATA_TYPES.NUMERIC else float(str(real_value).replace(',','.'))
        except:
            real_value = None

        if self.buckets is not None:
            predicted_value_b = get_value_bucket(predicted_value, self.buckets, self.col_stats, hmd)
            real_value_b = get_value_bucket(real_value, self.buckets, self.col_stats, hmd)
            X = [False] * (len(self.buckets) + 1)
            X[predicted_value_b] = True
            X = X + features_existence
            self._X_buff.append(X)

            self._Y_buff.append(real_value_b)

            # If no column is ignored, compute the accuracy for this bucket
            nr_missing_features = len([x for x in features_existence if x is False or x is 0])
            if nr_missing_features == 0:
                if predicted_value_b not in self.bucket_accuracy:
                    self.bucket_accuracy[predicted_value_b] = []
                self.bucket_accuracy[predicted_value_b].append(int(real_value_b == predicted_value_b))
        else:
            predicted_value_b = predicted_value
            real_value_b = real_value
            self._X_buff.append(features_existence)
            self._Y_buff.append(real_value_b == predicted_value_b)

    def get_accuracy_histogram(self):
        x = []
        y = []

        total_correct = 0
        total_vals = 0

        for bucket in self.bucket_accuracy:
            total_correct += sum(self.bucket_accuracy[bucket])
            total_vals += len(self.bucket_accuracy[bucket])
            x.append(bucket)
            y.append(sum(self.bucket_accuracy[bucket])/len(self.bucket_accuracy[bucket]))

        validation_set_accuracy = total_correct/total_vals
        return {
            'buckets': x
            ,'accuracies': y
        }, validation_set_accuracy


    def partial_fit(self):
        """
        # Fit the probabilistic validator on all observations recorder that haven't been taken into account yet
        """
        log_types = np.seterr()
        np.seterr(divide='ignore')

        if self.buckets is not None:
            self._probabilistic_model.partial_fit(self._X_buff, self._Y_buff, classes=self.bucket_keys)
        else:
            self._probabilistic_model.partial_fit(self._X_buff, self._Y_buff, classes=[True, False])

        np.seterr(divide=log_types['divide'])

        self._X_buff= []
        self._Y_buff= []

    def fit(self):
        """
        # Fit the probabilistic validator on all observations recorder that haven't been taken into account yet
        """
        log_types = np.seterr()
        np.seterr(divide='ignore')
        self._probabilistic_model.fit(self._X_buff, self._Y_buff)
        np.seterr(divide=log_types['divide'])

        self._X_buff= []
        self._Y_buff= []

    def evaluate_prediction_accuracy(self, features_existence, predicted_value):
        """
        # Fit the probabilistic validator on an observation    def evaluate_prediction_accuracy(self, features_existence, predicted_value):
        :param features_existence: A vector of 0 and 1 representing the existence of all the features (0 == not exists, 1 == exists)
        :param predicted_value: The predicted value/label
        :return: The probability (from 0 to 1) of our prediction being accurate (within the same histogram bucket as the real value)
        """
        if self.buckets is not None:
            predicted_value_b = get_value_bucket(predicted_value, self.buckets, self.col_stats)
            X = [False] * (len(self.buckets) + 1)
            X[predicted_value_b] = True
            X = [X + features_existence]
        else:
            X = [features_existence]

        distribution = self._probabilistic_model.predict_proba(np.array(X))[0]
        distribution = distribution.tolist()

        if len([x for x in distribution if x > 0.01]) > 4:
            # @HACK
            mean = np.mean(distribution)
            std = np.std(distribution)

            distribution = [x if x > (mean - std) else 0 for x in distribution]

            sum_dist = sum(distribution)
            distribution = [x/sum_dist for x in distribution]

            min_val = min([x for x in distribution if x > 0.001])
            distribution = [x - min_val if x > min_val else 0 for x in distribution]

            sum_dist = sum(distribution)
            distribution = [x/sum_dist for x in distribution]
            # @HACK
        else:
            pass


        if self.buckets is not None:
            return ProbabilityEvaluation(self.buckets, distribution, predicted_value)
        else:
            return distribution[1]



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
