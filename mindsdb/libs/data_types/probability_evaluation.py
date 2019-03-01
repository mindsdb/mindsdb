

class ProbabilityEvaluation:

    def __init__(self, buckets, evaluation_distribution = None, predicted_value = None, logger = None):

        self.distribution = evaluation_distribution
        self.predicted_value = predicted_value
        self.buckets = buckets
        self.most_likely_value = None
        self.most_likely_probability = None
        self.logger = logger

        if evaluation_distribution is not None:
            self.update(evaluation_distribution, predicted_value)

    def explain(self):

        data = {
            'x': [i if type(i) == type('') else "{0:.2f}".format(i) for i in self.buckets],
            'y': [i*100 for i in self.distribution],
            'label': 'The probability distribution of the prediction'
        }
        if self.logger is not None:
            self.logger.infoChart(data, type='histogram')



    def update(self, distribution, predicted_value):
        """
        For a given distribution, update the most_likely_values and probability
        :param distribution: the distribution values
        :return:
        """
        self.predicted_value = predicted_value
        self.distribution = distribution


        # now that we have a distribution obtain the most likely value and its probability
        max_prob_value = max(distribution)  # the highest probability in the distribution
        max_prob_index = distribution.index(max_prob_value)  # the index for the highest probability
        max_prob_bucket_left = self.buckets[
            max_prob_index]  # the predicted value will fall in between the two ends of the bucket (if not a text)

        if type(self.buckets[0]) == type(""):  # if we our buckets are text, then return the most likely label
            most_likely_value = max_prob_bucket_left
        else:  # else calcualte the value in between the buckets (optional future implementation: we can also calcualte a random value in between the buckets)
            if max_prob_index >= len(
                    self.buckets) - 1:  # if most likely is the far most right value in the buckets then dont average
                most_likely_value = max_prob_bucket_left
            else:
                max_prob_bucket_right = self.buckets[max_prob_index + 1]
                most_likely_value = max_prob_bucket_left + (max_prob_bucket_right - max_prob_bucket_left) / 2

        self.most_likely_value = most_likely_value
        self.most_likely_probability = max_prob_value
