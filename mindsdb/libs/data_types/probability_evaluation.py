

class ProbabilityEvaluation:

    def __init__(self, buckets, evaluation_distribution, predicted_value):
        self.distribution = evaluation_distribution
        self.predicted_value = predicted_value
        self.buckets = buckets
        self.most_likely_value = None
        self.most_likely_probability = None

        if evaluation_distribution is not None:
            self.update(evaluation_distribution, predicted_value)

    @staticmethod
    def get_ranges_with_confidences(distribution,buckets):
        peak_thr = min(0.12,max(distribution) - 0.01)
        memb_thr = min(peak_thr/2,0.06)
        clusters = []

        for i in range(len(distribution)):
            val = distribution[i]
            middle_bucket = buckets[i]

            vals = []
            poss = []
            cluster_buckets = []
            if val >= peak_thr:
                for i_prev in range(i - 1,0,-1):
                    if distribution[i_prev] < memb_thr:
                        break
                    vals.append(distribution[i_prev])
                    cluster_buckets.append(buckets[i_prev])
                    poss.append(i_prev)

                vals.reverse()
                poss.reverse()
                cluster_buckets.reverse()

                vals.append(val)
                poss.append(i)
                cluster_buckets.append(buckets[i])

                for i_next in range(i + 1,len(distribution),1):
                    if distribution[i_next] < memb_thr:
                        break
                    vals.append(distribution[i_next])
                    poss.append(i_next)
                    cluster_buckets.append(buckets[i_next])

                clusters.append({'values':vals,'positions':poss,'middle':val, 'buckets':cluster_buckets, 'confidence':sum(vals), 'middle_bucket': middle_bucket})

        i = 0
        while i < len(clusters):
            broke = False
            for ii in range(len(clusters)):
                if i != ii:
                    if len(set(clusters[i]['positions']).intersection(clusters[ii]['positions'])) > 0:
                        broke = True
                        if clusters[i]['middle'] > clusters[ii]['middle']:
                            del clusters[ii]
                        else:
                            del clusters[i]
                        break
            if broke:
                continue
            i += 1
        return clusters


    def explain(self):
        clusters = self.get_ranges_with_confidences(self.distribution,self.buckets)
        for i in range(len(clusters)):
            clusters[i]['predicted_value'] = self.predicted_value

        return clusters


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
        max_prob_bucket_left = self.buckets[max_prob_index]  # the predicted value will fall in between the two ends of the bucket (if not a text)

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
