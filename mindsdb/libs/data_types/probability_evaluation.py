from mindsdb.libs.constants.mindsdb import *


class ProbabilityEvaluation:

    def __init__(self, buckets, evaluation_distribution, predicted_value):
        self.distribution = evaluation_distribution
        self.predicted_value = predicted_value
        self.buckets = buckets
        self.most_likely_value = None
        self.most_likely_probability = None
        self.final_value = None

        if evaluation_distribution is not None:
            self.update(evaluation_distribution, predicted_value)

    @staticmethod
    def get_ranges_with_confidences(distribution,buckets,predicted_value, col_stats):
        peak_thr = min(0.12,max(distribution) - 0.01)
        memb_thr = min(peak_thr/2,0.06)
        clusters = []

        for i in range(len(distribution)):
            val = distribution[i]
            middle_bucket = buckets[i]
            middle_bucket_left = None

            vals = []
            poss = []
            cluster_buckets = []
            if val >= peak_thr:

                if col_stats['data_type'] in (DATA_TYPES.NUMERIC, DATA_TYPES.DATE):
                    for i_prev in range(i - 1,0,-1):
                        # Here we break afterwards since a bucket has as "limits" it's value (max) and the value of the previous bucket(min)
                        if distribution[i_prev] < memb_thr or distribution[i_prev] < val/5:
                            if i_prev == i
                                if i_prev == 0:
                                    middle_bucket_left = 0
                                else:
                                    middle_bucket_left = buckets[i_prev - 1]
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

                if col_stats['data_type'] in (DATA_TYPES.NUMERIC, DATA_TYPES.DATE):
                    for i_next in range(i + 1,len(distribution),1):
                        if distribution[i_next] < memb_thr:
                            break
                        vals.append(distribution[i_next])
                        poss.append(i_next)
                        cluster_buckets.append(buckets[i_next])

                clusters.append({'values':vals,'positions':poss,'middle_confidence':val, 'buckets':cluster_buckets, 'confidence':sum(vals), 'middle_bucket_right': middle_bucket, 'middle_position': i, 'middle_bucket_left':middle_bucket_left})

        i = 0
        while i < len(clusters):
            broke = False
            for ii in range(len(clusters)):
                if i != ii:
                    if len(set(clusters[i]['positions']).intersection(clusters[ii]['positions'])) > 0:
                        broke = True
                        if clusters[i]['middle_confidence'] > clusters[ii]['middle_confidence']:
                            del clusters[ii]
                        else:
                            del clusters[i]
                        break
            if broke:
                continue
            i += 1

        return clusters


    def explain(self, col_stats):
        self.update(self.distribution, self.predicted_value)
        if self.buckets is None:
            clusters = [{
                    'middle_confidence':self.most_likely_probability,
                    'confidence':self.most_likely_probability,
                    'middle_bucket': self.most_likely_value,
                    'value': self.final_value
            }]
        else:
            clusters = self.get_ranges_with_confidences(self.distribution,self.buckets,self.predicted_value, col_stats)

            for i in range(len(clusters)):
                if self.final_value == clusters[i] or self.final_value
                clusters[i]['value'] = self.final_value

        return clusters


    def update(self, distribution, predicted_value):
        """
        For a given distribution, update the most_likely_values and probability
        :param distribution: the distribution values
        :return:
        """
        self.predicted_value = predicted_value
        self.distribution = distribution

        if self.buckets is None:
            self.most_likely_probability = self.distribution[0]

        # now that we have a distribution obtain the most likely value and its probability
        self.most_likely_probability = max(self.distribution)  # the highest probability in the distribution
        max_prob_index = self.distribution.index(self.most_likely_probability)  # the index for the highest probability
        bucket_margin_right = self.buckets[max_prob_index]  # the predicted value will fall in between the two ends of the bucket (if not a text)

        # if we our buckets are text, then return the most likely label
        if  type(self.buckets[0]) == type(""):
            print(self.distribution)
            self.most_likely_value = bucket_margin_right
            self.final_value = self.most_likely_value
        # else calcualte the value in between the buckets (optional future implementation: we can also calcualte a random value in between the buckets)
        else:
            # if most likely is the far most right value in the buckets then dont average
            bucket_margin_left = None
            if max_prob_index >= len(self.buckets) - 1:
                self.most_likely_value = bucket_margin_right
                self.final_value = self.most_likely_value
            else:
                bucket_margin_left = self.buckets[max_prob_index - 1]
                self.most_likely_value = (bucket_margin_right + bucket_margin_left)/2

                if bucket_margin_left is None and predicted_value <= bucket_margin_right:
                    self.final_value = predicted_value
                elif predicted_value <= bucket_margin_right and predicted_value >= bucket_margin_left:
                    self.final_value = predicted_value
                else:
                    self.final_value = self.most_likely_value
