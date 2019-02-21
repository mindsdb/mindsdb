from sklearn.naive_bayes import GaussianNB, ComplementNB, MultinomialNB
from mindsdb.libs.constants.mindsdb import NULL_VALUES
import numpy as np
import pickle

"""
# @TODO: Figure out how to source the histogram and chang the _get_value_bucket
function and the interface of register_observation observation accordingly

# @TODO Figure out how and if to adapt the register_observation and evaluate_prediction_accuracy
functions to work with vectors of features and labels (i.e. multiple rows at once)
This could increase efficiency
"""
class ProbabilisticValidator():
    _smoothing_factor = 1
    _value_bucket_probabilities = {}
    _probabilistic_model = None


    def __init__(self):
        # <--- Pick one of the 3
        self._probabilistic_model = ComplementNB(alpha=self._smoothing_factor)
        #self._probabilistic_model = GaussianNB()
        #self._probabilistic_model = MultinomialNB()

    def pickle(self):
        return pickle.dumps(self)

    @staticmethod
    def unpickle(binary):
        return pickle.loads(binary.encode(encoding='latin1'))

    @staticmethod
    def closest(arr, value):
        aux = []
        for ele in arr:
            aux.append(abs(value-ele))
        return aux.index(min(aux))

    # For contignous values we want to use a bucket in the histogram to get a discrete label
    def _get_value_bucket(self, value, histogram):
        # @TODO Not implemented
        i = self.closest(histogram['x'], value)
        return histogram['y'][i]


    def register_observation(self, features_existence, real_value, predicted_value, histogram):
        real_value_b = self._get_value_bucket(real_value, histogram)
        predicted_value_b = self._get_value_bucket(predicted_value, histogram)

        correct_prediction = real_value_b == predicted_value_b

        X = [predicted_value_b, *features_existence]
        Y = [correct_prediction]

        self._probabilistic_model.partial_fit(np.array(X).reshape(1,-1), Y, classes=[True, False])


    def evaluate_prediction_accuracy(self, features_existence, predicted_value, histogram):
        predicted_value_b = self._get_value_bucket(predicted_value, histogram)

        X = [predicted_value_b, *features_existence]

        return self._probabilistic_model.predict_proba(np.array(X).reshape(1,-1))[0][1]


if __name__ == "__main__":
    feature_rows = [
        [None,2,3]
        ,[2,2,3]
        ,[1,None,6]
        ,[0,3,None]
        ,[2,0,1]
        ,[None,2,3]
        ,[2,2,3]
        ,[1,None,6]
        ,[0,3,None]
        ,[2,0,1]
        ]

    values = [2,2,2,3,5,2,2,2,3,5]
    predictions = [2,2,2,3,2,2,2,2,3,2]

    pbv = ProbabilisticValidator()

    for i in range(len(feature_rows)):
        pbv.register_observation(feature_rows[i],values[i], predictions[i])

    print(pbv.evaluate_prediction_accuracy([1,2,3], 2))
    print(pbv.evaluate_prediction_accuracy([1,None,3], 3))
    print(pbv.evaluate_prediction_accuracy([None,0,2], 5))
    print(pbv.evaluate_prediction_accuracy([None,2,3], 2))
    print(pbv.evaluate_prediction_accuracy([None,None,3], 2))
    print(pbv.evaluate_prediction_accuracy([2,2,None], 2))
