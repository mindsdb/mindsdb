from sklearn.naive_bayes import MultinomialNB
import numpy as np

"""
# @TODO: Figure out how to source the histogram and chang the _get_value_bucket
function and the interface of register_observation observation accordingly

# @TODO Figure out how and if to adapt the register_observation and evaluate_prediction_accuracy
functions to work with vectors of features and labels (i.e. multiple rows at once)
This could increase efficiency
"""
class ProbabilisticValidator():
    _smoothing_factor = 0.2
    _value_bucket_probabilities = {}
    _probabilistic_model = None
    _use_features = None

    def __init__(self, value_stats, use_features=False):
        self._use_features = use_features

        if self._use_features:
            self._probabilistic_model = MultinomialNB(alpha=self._smoothing_factor)

        for bucket in value_stats['histogram']:
            if value_stats['histogram'][bucket] > 0:
                self._value_bucket_probabilities[bucket] = {
                    'True': self._smoothing_factor , 'False': self._smoothing_factor, 'nr_observations': self._smoothing_factor
                }
            else:
                self._value_bucket_probabilities[bucket] = {
                    'True': 0 , 'False': 0, 'nr_observations': 0.00001
                }

    # For contignous values we want to use a bucket in the histogram to get a discrete label
    @staticmethod
    def _get_value_bucket(value, histogram):
        # @TODO Not implemented
        return value

    def register_observation(self, features, real_value, predicted_value):
        real_value_b = self._get_value_bucket(real_value, None)
        predicted_value_b = self._get_value_bucket(predicted_value, None)

        correct_prediction = real_value_b == predicted_value_b

        if self._use_features:
            features_b = list(map(lambda x: self._get_value_bucket(x, None), features))
            features_b.append(predicted_value_b)
            self._probabilistic_model.partial_fit(np.array(features_b).reshape(1,-1), np.array([correct_prediction]) ,classes=[False, True])
        else:
            self._value_bucket_probabilities[predicted_value_b][str(correct_prediction)] += 1
            self._value_bucket_probabilities[predicted_value_b]['nr_observations'] += 1


    def evaluate_prediction_accuracy(self, features, predicted_value):
        predicted_value_b = self._get_value_bucket(predicted_value, None)

        if self._use_features:
            features_b = list(map(lambda x: self._get_value_bucket(x, None), features))
            features_b.append(predicted_value_b)
            """
            # Note: probabilities are rturned for the sorted vector of classes
            So, in thise case, since True(1) > False(0) the second value in the vecotr
            is the probability predicted for 'True' (i.e. chance of our prediction being true)
            """
            return self._probabilistic_model.predict_proba(np.array(features_b).reshape(1,-1))[0][1]
        else:
            try:
                return self._value_bucket_probabilities[predicted_value_b]['True'] / self._value_bucket_probabilities[predicted_value_b]['nr_observations']
            except:
                return 0


if __name__ == "__main__":
    feature_rows = [
        [1,2,3]
        ,[2,2,3]
        ,[1,2,6]
        ,[0,3,3]
        ,[2,0,1]
    ]

    values = [2,2,2,3,5]
    predictions = [2,2,2,3,2]

    value_col_stats = {'histogram': {
        1: 111
        ,2: 212
        ,3: 17
        ,4: 2
        ,5: 6
        ,6: 0
        ,7: 0
    }}

    pbv = ProbabilisticValidator(value_col_stats, use_features=True)

    for i in range(len(feature_rows)):
        pbv.register_observation(feature_rows[i],values[i], predictions[i])

    print(pbv.evaluate_prediction_accuracy([1,2,3], 2))
    print(pbv.evaluate_prediction_accuracy([1,2,3], 3))
    print(pbv.evaluate_prediction_accuracy([1,2,6], 5))
    print(pbv.evaluate_prediction_accuracy([22,12,61], 101))
