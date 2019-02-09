

class ProbabilisticValidator():
    _smoothing_factor = 1
    _value_bucket_probabilities = {}

    # For contignous values we want to use a bucket in the histogram to get a discrete label
    @staticmethod
    def _get_value_bucket(value, histogram):
        # @TODO Not implemented
        return value


    def __init__(self, value_stats):
        for bucket in value_stats['histogram']:
            if value_stats['histogram'][bucket] > 0:
                self._value_bucket_probabilities[bucket] = {
                    'True': self._smoothing_factor , 'False': self._smoothing_factor, 'nr_observations': self._smoothing_factor
                }
            else:
                self._value_bucket_probabilities[bucket] = {
                    'True': 0 , 'False': 0, 'nr_observations': 0.00001
                }

    def register_observation(self, features, real_value, predicted_value):
        real_value_b = self._get_value_bucket(real_value, None)
        predicted_value_b = self._get_value_bucket(predicted_value, None)

        correct_prediction = real_value_b == predicted_value_b

        """
        # @TODO: In the future just do this once for each value in the histogram
        # That way we also apply the smoothing to all values,
            in order to deal with values that don't occur at all
        # This will probably happen in the __init__ using the histogram of the values from the training set
        """
        if predicted_value_b not in self._value_bucket_probabilities:
            self._value_bucket_probabilities[predicted_value_b] = {
                'True': self._smoothing_factor , 'False': self._smoothing_factor, 'nr_observations': self._smoothing_factor
            }

        self._value_bucket_probabilities[predicted_value_b][str(correct_prediction)] += 1
        self._value_bucket_probabilities[predicted_value_b]['nr_observations'] += 1


    def evaluate_prediction_accuracy(self, features, predicted_value):
        predicted_value_b = self._get_value_bucket(predicted_value, None)

        try:
            chance_of_accuracy = self._value_bucket_probabilities[predicted_value_b]['True'] / self._value_bucket_probabilities[predicted_value_b]['nr_observations']
        except:
            chance_of_accuracy = 0

        return chance_of_accuracy


if __name__ == "__main__":
    feature_rows = [
        [1,2,3]
        ,[2,2,3]
        ,[1,2,6]
        ,[None,3,3]
        ,[2,None,1]
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

    pbv = ProbabilisticValidator(value_col_stats)

    for i in range(len(feature_rows)):
        pbv.register_observation(feature_rows[i],values[i], predictions[i])

    print(pbv.evaluate_prediction_accuracy([], 2))
    print(pbv.evaluate_prediction_accuracy([], 3))
    print(pbv.evaluate_prediction_accuracy([], 5))
    print(pbv.evaluate_prediction_accuracy([], 101))
