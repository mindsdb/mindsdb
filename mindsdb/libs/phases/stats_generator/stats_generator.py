"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

import random
import warnings
from collections import Counter

import numpy as np
import scipy.stats as st
from dateutil.parser import parse as parseDate
from sklearn.neighbors import LocalOutlierFactor
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import matthews_corrcoef

from mindsdb.config import CONFIG

from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive, clean_float, cast_string_to_python_type
from mindsdb.external_libs.stats import calculate_sample_size

from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata


class StatsGenerator(BaseModule):
    """
    # The stats generator phase is responsible for generating the insights we need about the data in order to vectorize it
    # Additionally, the stats generator also provides the user with some extra meaningful information about his data,
    thoguh this functionality may be moved to a different step (after vectorization) in the future
    """

    phase_name = PHASE_STATS_GENERATOR

    def _is_number(self, string):
        """ Returns True if string is a number. """
        try:
            clean_float(string)
            return True
        except ValueError:
            return False

    def _is_date(self, string):
        """ Returns True if string is a valid date format """
        try:
            parseDate(string)
            return True
        except ValueError:
            return False

    def _get_text_type(self, data):
        """
        Takes in column data and defiens if its categorical or full_text

        :param data: a list of cells in a column
        :return: DATA_TYPES.CATEGORICAL or DATA_TYPES.FULL_TEXT
        """

        total_length = len(data)
        key_count = {}
        max_number_of_words = 0

        for cell in data:

            if cell not in key_count:
                key_count[cell] = 1
            else:
                key_count[cell] += 1

            cell_wseparator = cell
            sep_tag = '{#SEP#}'
            for separator in WORD_SEPARATORS:
                cell_wseparator = str(cell_wseparator).replace(separator,sep_tag)

            words_split = cell_wseparator.split(sep_tag)
            words = len([ word for word in words_split if word not in ['', None] ])

            if max_number_of_words < words:
                max_number_of_words += words

        if max_number_of_words == 1:
            return DATA_TYPES.CATEGORICAL
        if max_number_of_words <= 3 and len(key_count) < total_length * 0.8:
            return DATA_TYPES.CATEGORICAL
        else:
            return DATA_TYPES.TEXT


    def _get_column_data_type(self, data):
        """
        Provided the column data, define it its numeric, data or class

        :param data: a list containing each of the cells in a column

        :return: type and type distribution, we can later use type_distribution to determine data quality
        NOTE: type distribution is the count that this column has for belonging cells to each DATA_TYPE
        """

        currentGuess = None

        type_dist = {}

        # calculate type_dist
        for element in data:
            if self._is_number(element):
                currentGuess = DATA_TYPES.NUMERIC
            elif self._is_date(element):
                currentGuess = DATA_TYPES.DATE
            else:
                currentGuess = DATA_TYPES.TEXT

            if currentGuess not in type_dist:
                type_dist[currentGuess] = 1
            else:
                type_dist[currentGuess] += 1

        curr_data_type = DATA_TYPES.TEXT
        max_data_type = 0

        # assume that the type is the one with the most prevelant type_dist
        for data_type in type_dist:
            if type_dist[data_type] > max_data_type:
                curr_data_type = data_type
                max_data_type = type_dist[data_type]

        #TODO: If there are cell values that dont match the prevelant type, we should log this information

        # If it finds that the type is categorical it should determine if its categorical or actual text
        if curr_data_type == DATA_TYPES.TEXT:
            return self._get_text_type(data), type_dist

        return curr_data_type, type_dist






    def _get_words_dictionary(self, data, full_text = False):
        """ Returns an array of all the words that appear in the dataset and the number of times each word appears in the dataset """

        splitter = lambda w, t: [wi.split(t) for wi in w] if type(w) == type([]) else splitter(w,t)

        if full_text:
            # get all words in every cell and then calculate histograms
            words = []
            for cell in data:
                words += splitRecursive(cell, WORD_SEPARATORS)

            hist = {i: words.count(i) for i in words}
            x = list(hist.keys())
            histogram = {
                'x': x,
                'y': list(hist.values())
            }
            return x, histogram


        else:
            hist = {i: data.count(i) for i in data}
            x = list(hist.keys())
            histogram = {
                'x': x,
                'y': list(hist.values())
            }
            return x, histogram

    # @TODO Use or move to scraps.py
    def _get_params_as_dictionary(self, params):
        """ Returns a dictionary with the params of the distribution """
        arg = params[:-2]
        loc = params[-2]
        scale = params[-1]
        ret = {
            'loc': loc,
            'scale': scale,
            'shape': arg
        }
        return ret

    def _compute_value_distribution_score(self, stats, columns, col_name):
        """
        # Looks at the histogram and transforms it into a proability mapping for each
        bucket, then generates a quality score (value_distribution_score) based on that

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            bucket_probabilities: A value distribution score, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
            value_distribution_score: A dictioanry of with the probabilities that a value values in a bucket, for each of he buckets in the histogram
        """

        bucket_probabilities = {}
        pair = stats[col_name]['histogram']
        total_vals = sum(pair['y'])
        for i in range(len(pair['x'])):
            bucket_probabilities[pair['x'][i]] = pair['y'][i]/total_vals

        probabilities = list(bucket_probabilities.values())

        max_probability = max(probabilities)
        max_probability_key = max(bucket_probabilities, key=lambda k: bucket_probabilities[k])

        value_distribution_score = 1 - np.mean(probabilities)/max_probability

        data = {
            'bucket_probabilities': bucket_probabilities
            ,'value_distribution_score': value_distribution_score
            ,'max_probability_key': max_probability_key
        }

        return data


    def _compute_duplicates_score(self, stats, columns, col_name):
        """
        # Looks at the set of distinct values for all the data and computes a quality
        socre based on how many of the values are duplicates

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            nr_duplicates: the nr of cells which contain values that are found more than once
            duplicates_percentage: % of the values that are found more than once
            duplicate_score: a quality based on the duplicate percentage, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """

        occurances = Counter(columns[col_name])
        values_that_occur_twice_or_more = filter(lambda val: occurances[val] < 2, occurances)
        nr_of_occurances = map(lambda val: occurances[val], values_that_occur_twice_or_more)
        nr_duplicates = sum(nr_of_occurances)
        data = {
            'nr_duplicates': nr_duplicates
            ,'duplicates_percentage': nr_duplicates*100/len(columns[col_name])
        }

        if stats[col_name][KEYS.DATA_TYPE] != DATA_TYPES.CATEGORICAL and stats[col_name][KEYS.DATA_TYPE] != DATA_TYPES.DATE:
            data['duplicate_score'] = data['duplicates_percentage']/100
        else:
            data['c'] = 0

        return data

    def _compute_empty_cells_score(self, stats, columns, col_name):
        """
        # Creates a quality socre based on the percentage of empty cells (empty_percentage)

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            empty_cells_score: A quality score based on the nr of empty cells, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """

        return {'empty_cells_score': stats[col_name]['empty_percentage']/100}

    def _compute_data_type_dist_score(self, stats, columns, col_name):
        """
        # Creates a quality socre based on the data type distribution, this score is based on
        the difference between the nr of values with the "main" data type
        and all the nr of values with all other data types

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            data_type_distribution_score: A quality score based on the nr of empty cells, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """

        vals = stats[col_name]['data_type_dist'].values()
        principal = max(vals)
        total = len(columns[col_name])
        data_type_dist_score = (total - principal)/total
        return {'data_type_distribution_score': data_type_dist_score}

    def _compute_z_score(self, stats, columns, col_name):
        """
        # Computes the z_score for each value in our column.
        # This score represents the distance from the mean over the standard deviation.
        # Based on this, compute a quality metrics.

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            z_score_outliers: The indexs of values which we consider outliers based on the z score
            mean_z_score: The mean z score for the column
            z_test_based_outlier_score: A quality score based on the nr of outliers as determined by their z score, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """
        if stats[col_name][KEYS.DATA_TYPE] != DATA_TYPES.NUMERIC:
            return {}

        z_scores = list(map(abs,(st.zscore(columns[col_name]))))
        threshold = 3
        z_score_outlier_indexes = [i for i in range(len(z_scores)) if z_scores[i] > threshold]
        data = {
            'z_score_outliers': z_score_outlier_indexes
            ,'mean_z_score': np.mean(z_scores)
            ,'z_test_based_outlier_score': len(z_score_outlier_indexes)/len(columns[col_name])
        }
        return data

    def _compute_lof_score(self, stats, columns, col_name):
        """
        # Uses LocalOutlierFactor (a KNN clustering based method from sklearn)
        to determine outliers within our column
        # All data that has a small score after we call `fit_predict` has a high chance of being an outlier
        based on the distance from the clusters created by LOF

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            lof_outliers: The indexs of values which we consider outliers based on LOF
            lof_based_outlier_score: A quality score based on the nr of outliers as determined by their LOF score, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """

        if stats[col_name][KEYS.DATA_TYPE] != DATA_TYPES.NUMERIC:
            return {}

        np_col_data = np.array(columns[col_name]).reshape(-1, 1)
        lof = LocalOutlierFactor(contamination='auto')
        outlier_scores = lof.fit_predict(np_col_data)
        outlier_indexes = [i for i in range(len(columns[col_name])) if outlier_scores[i] < -0.8]

        return {
            'lof_outliers': outlier_indexes
            ,'lof_based_outlier_score': len(outlier_indexes)/len(columns[col_name])
            ,'percentage_of_log_based_outliers': (len(outlier_indexes)/len(columns[col_name])) * 100
        }


    def _compute_similariy_score(self, stats, columns, col_name):
        """
        # Uses matthews correlation to determine up what % of their cells two columns are identical

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            similarities: How similar this column is to other columns (with 0 being completely different and 1 being an exact copy).
            similarity_score: A score equal to the highest similarity found, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """
        col_data = columns[col_name]

        similarities = []
        for _, other_col_name in enumerate(columns):
            if other_col_name == col_name:
                continue
            else:
                similarity = matthews_corrcoef(list(map(str,col_data)), list(map(str,columns[other_col_name])))
                similarities.append((other_col_name,similarity))

        max_similarity = max(map(lambda x: x[1], similarities))

        return {
            'similarities': similarities
            ,'similarity_score': max_similarity
            ,'most_similar_column_name': list(filter(lambda x: x[1] == max_similarity, similarities))[0][0]
        }


    def _compute_clf_based_correlation_score(self, stats, columns, col_name):
        """
        # Tries to find correlated columns by trying to predict the values in one
        column based on all the others using a simple DT classifier
        # The purpose of this is not to see if a column is predictable, but rather, to
        see if it can be predicted accurately based on a single other column, or if
        all other columns play an equally important role
        # A good prediction score, based on all the columns, doesn't necessarily imply
        a correlation between columns, it could just mean the data is very garnular (few repeat values)

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            correlation_score: A score equal to the prediction accuracy * the importance (from 0 to 1)
                of the most imporant column in making said prediciton,
                ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
            highest_correlation: The importance of the most_correlated_column in the DT classifier
            most_correlated_column: The column with which our column is correlated most based on the DT
        """
        full_col_data = columns[col_name]

        dt_clf = DecisionTreeClassifier()

        other_feature_names = []
        other_features = []
        for _, other_col_name in enumerate(columns):
            if other_col_name == col_name:
                continue

            other_feature_names.append(other_col_name)
            le = LabelEncoder()
            _stringified_col = list(map(str,columns[other_col_name]))
            le.fit(_stringified_col)
            other_features.append(list(le.transform(_stringified_col)))

        other_features_t = np.array(other_features, dtype=object).transpose()

        le = LabelEncoder()
        _stringified_col = list(map(str,full_col_data))
        le.fit(_stringified_col)
        y = le.transform(_stringified_col)
        dt_clf.fit(other_features_t,y)
        prediction_score = dt_clf.score(other_features_t,y)
        corr_scores = list(dt_clf.feature_importances_)
        highest_correlated_column = max(corr_scores)
        return {
            'correlation_score': prediction_score * highest_correlated_column
            ,'highest_correlation': max(corr_scores)
            ,'most_correlated_column': other_feature_names[corr_scores.index(max(corr_scores))]
        }

    def _compute_data_quality_score(self, stats, columns, col_name):
        """
        # Attempts to determine the quality of the column through aggregating all quality score
        we could compute about it

        :param stats: The stats extracted up until this point for all columns
        :param columns: All the columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            quality_score: An aggreagted quality socre that attempts to asses the overall quality of the column,
                , ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
            bad_scores: The socres which lead to use rating this column poorly
        """
        scores_used = 0
        scores_total = 0
        scores = ['correlation_score', 'lof_based_outlier_score', 'z_test_based_outlier_score', 'data_type_distribution_score', 'empty_cells_score', 'duplicate_score', 'similarity_score', 'value_distribution_score']
        for score in scores:
            if score in stats[col_name]:
                scores_used += 1
                scores_total += stats[col_name][score]

        quality_score = scores_total/scores_used
        bad_scores = []
        for score in scores:
            if score in stats[col_name]:
                if stats[col_name][score] > quality_score:
                    bad_scores.append(score)
        return {'quality_score': quality_score, 'bad_scores': bad_scores}

    def _log_interesting_stats(self, stats):
        """
        # Provide interesting insights about the data to the user and send them to the logging server in order for it to generate charts

        :param stats: The stats extracted up until this point for all columns
        """
        for col_name in stats:
            col_stats = stats[col_name]

            # Overall quality
            if col_stats['quality_score'] > 0.5:
                # Some scores are not that useful on their own, so we should only warn users about them if overall quality is bad.
                self.log.warning('Column "{}" is considered of low quality, the scores that influenced this decission are: {}'.format(col_name, col_stats['bad_scores']))
                if col_stats['duplicates_score'] > 0.5:
                    duplicates_percentage = col_stats['duplicates_percentage']
                    self.log.warning(f'{duplicates_percentage}% of the values in column {col_name} seem to be repeated, this might indicate your data is of poor quality.')


            # Some scores are meaningful on their own, and the user should be warnned if they fall bellow a certain threshold
            if col_stats['empty_cells_score'] > 0.2:
                empty_cells_percentage = col_stats['empty_percentage']
                self.log.warning(f'{empty_cells_percentage}% of the values in column {col_name} are empty, this might indicate your data is of poor quality.')

            if col_stats['data_type_distribution_score'] > 0.2:
                #self.log.infoChart(stats[col_name]['data_type_dist'], type='list', uid='Dubious Data Type Distribution for column "{}"'.format(col_name))
                percentage_of_data_not_of_principal_type = col_stats['data_type_distribution_score'] * 100
                principal_data_type = col_stats[KEYS.DATA_TYPE]
                self.log.warning(f'{percentage_of_data_not_of_principal_type}% of your data is not of type {principal_data_type}, which was detected to be the data type for column {col_name}, this might indicate your data is of poor quality.')

            if 'z_test_based_outlier_score' in col_stats and col_stats['z_test_based_outlier_score'] > 0.3:
                percentage_of_outliers = col_stats['z_test_based_outlier_score']*100
                self.log.warning(f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data is more than 3 standard deviations away from the mean, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it.""")

            if 'lof_based_outlier_score' in col_stats and col_stats['lof_based_outlier_score'] > 0.3:
                percentage_of_outliers = col_stats['percentage_of_log_based_outliers']
                self.log.warning(f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data doesn't fit closely in any cluster using the KNN algorithm (20n) to cluster your data, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it.""")

            if col_stats['value_distribution_score'] > 0.8:
                max_probability_key = col_stats['max_probability_key']
                self.log.warning(f"""Column {col_name} is very biased towards the value {max_probability_key}, please make sure that the data in this column is correct !""")

            if col_stats['similarity_score'] > 0.5:
                similar_percentage = col_stats['similarity_score'] * 100
                similar_col_name = col_stats['most_similar_column_name']
                self.log.warning(f'Column {col_name} and {similar_col_name} are {similar_percentage}% the same, please make sure these represent two distinct features of your data !')

            if col_stats['correlation_score'] > 0.4:
                not_quite_correlation_percentage = col_stats['correlation_score'] * 100
                most_correlated_column = col_stats['most_correlated_column']
                self.log.warning(f"""Using a statistical predictor we\'v discovered a correlation of roughly {not_quite_correlation_percentage}% between column
                {col_name} and column {most_correlated_column}""")


            # We might want to inform the user about a few stats regarding his column regardless of the score, this is done bellow
            self.log.info('Data distribution for column "{}"'.format(col_name))
            self.log.infoChart(stats[col_name]['data_type_dist'], type='list', uid='Data Type Distribution for column "{}"'.format(col_name))


    def run(self):
        """
        # Runs the stats generation phase
        # This shouldn't alter the columns themselves, but rather provide the `stats` metadata object and update the types for each column
        # A lot of information about the data distribution and quality will  also be logged to the server in this phase
        """
        self.train_meta_data = TransactionMetadata()
        self.train_meta_data.setFromDict(self.transaction.persistent_model_metadata.train_metadata)

        header = self.transaction.input_data.columns
        non_null_data = {}
        all_sampled_data = {}

        for column in header:
            non_null_data[column] = []
            all_sampled_data[column] = []

        empty_count = {}
        column_count = {}

        # we dont need to generate statistic over all of the data, so we subsample, based on our accepted margin of error
        population_size = len(self.transaction.input_data.data_array)
        sample_size = int(calculate_sample_size(population_size=population_size, margin_error=CONFIG.DEFAULT_MARGIN_OF_ERROR, confidence_level=CONFIG.DEFAULT_CONFIDENCE_LEVEL))

        # get the indexes of randomly selected rows given the population size
        input_data_sample_indexes = random.sample(range(population_size), sample_size)
        self.log.info('population_size={population_size},  sample_size={sample_size}  {percent:.2f}%'.format(population_size=population_size, sample_size=sample_size, percent=(sample_size/population_size)*100))

        for sample_i in input_data_sample_indexes:
            row = self.transaction.input_data.data_array[sample_i]

            for i, val in enumerate(row):
                column = header[i]
                value = cast_string_to_python_type(val)
                if not column in empty_count:
                    empty_count[column] = 0
                    column_count[column] = 0
                if value == None:
                    empty_count[column] += 1
                else:
                    non_null_data[column].append(value)
                all_sampled_data[column].append(value)
                column_count[column] += 1
        stats = {}

        for i, col_name in enumerate(non_null_data):
            col_data = non_null_data[col_name] # all rows in just one column
            full_col_data = all_sampled_data[col_name]
            data_type, data_type_dist = self._get_column_data_type(col_data)

            # NOTE: Enable this if you want to assume that some numeric values can be text
            # We noticed that by default this should not be the behavior
            # TODO: Evaluate if we want to specify the problem type on predict statement as regression or classification
            #
            # if col_name in self.train_meta_data.model_predict_columns and data_type == DATA_TYPES.NUMERIC:
            #     unique_count = len(set(col_data))
            #     if unique_count <= CONFIG.ASSUME_NUMERIC_AS_TEXT_WHEN_UNIQUES_IS_LESS_THAN:
            #         data_type = DATA_TYPES.CLASS

            # Generic stats that can be generated for any data type


            if data_type == DATA_TYPES.DATE:
                for i, element in enumerate(col_data):
                    if str(element) in [str(''), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA']:
                        col_data[i] = None
                    else:
                        try:
                            col_data[i] = int(parseDate(element).timestamp())
                        except:
                            self.log.warning('Could not convert string to date and it was expected, current value {value}'.format(value=element))
                            col_data[i] = None

            if data_type == DATA_TYPES.NUMERIC or data_type == DATA_TYPES.DATE:
                newData = []

                for value in col_data:
                    if value != '' and value != '\r' and value != '\n':
                        newData.append(value)


                col_data = [clean_float(i) for i in newData if str(i) not in ['', str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA']]

                y, x = np.histogram(col_data, 50, density=False)
                x = (x + np.roll(x, -1))[:-1] / 2.0
                x = x.tolist()
                y = y.tolist()

                xp = []

                if len(col_data) > 0:
                    max_value = max(col_data)
                    min_value = min(col_data)
                    mean = np.mean(col_data)
                    median = np.median(col_data)
                    var = np.var(col_data)
                    skew = st.skew(col_data)
                    kurtosis = st.kurtosis(col_data)


                    inc_rate = 0.05
                    initial_step_size = abs(max_value-min_value)/100

                    xp += [min_value]
                    i = min_value + initial_step_size

                    while i < max_value:

                        xp += [i]
                        i_inc = abs(i-min_value)*inc_rate
                        i = i + i_inc


                    # TODO: Solve inc_rate for N
                    #    min*inx_rate + (min+min*inc_rate)*inc_rate + (min+(min+min*inc_rate)*inc_rate)*inc_rate ....
                    #
                    #      x_0 = 0
                    #      x_i = (min+x_(i-1)) * inc_rate = min*inc_rate + x_(i-1)*inc_rate
                    #
                    #      sum of x_i_{i=1}^n (x_i) = max_value = inc_rate ( n * min + sum(x_(i-1)) )
                    #
                    #      mx_value/inc_rate = n*min + inc_rate ( n * min + sum(x_(i-2)) )
                    #
                    #     mx_value = n*min*in_rate + inc_rate^2*n*min + inc_rate^2*sum(x_(i-2))
                    #              = n*min(inc_rate+inc_rate^2) + inc_rate^2*sum(x_(i-2))
                    #              = n*min(inc_rate+inc_rate^2) + inc_rate^2*(inc_rate ( n * min + sum(x_(i-3)) ))
                    #              = n*min(sum_(i=1)^(i=n)(inc_rate^i))
                    #    =>  sum_(i=1)^(i=n)(inc_rate^i)) = max_value/(n*min(sum_(i=1)^(i=n))
                    #
                    # # i + i*x

                else:
                    max_value = 0
                    min_value = 0
                    mean = 0
                    median = 0
                    var = 0
                    skew = 0
                    kurtosis = 0
                    xp = []


                is_float = True if max([1 if int(i) != i else 0 for i in col_data]) == 1 else False


                col_stats = {
                    KEYS.DATA_TYPE: data_type,
                    "mean": mean,
                    "median": median,
                    "variance": var,
                    "skewness": skew,
                    "kurtosis": kurtosis,
                    "max": max_value,
                    "min": min_value,
                    "is_float": is_float,
                    "histogram": {
                        "x": x,
                        "y": y
                    },
                    "percentage_buckets": xp
                }
                stats[col_name] = col_stats
            # else if its text
            else:
                # see if its a sentence or a word
                is_full_text = True if data_type == DATA_TYPES.TEXT else False
                dictionary, histogram = self._get_words_dictionary(col_data, is_full_text)

                # if no words, then no dictionary
                if len(col_data) == 0:
                    dictionary_available = False
                    dictionary_lenght_percentage = 0
                    dictionary = []
                else:
                    dictionary_available = True
                    dictionary_lenght_percentage = len(
                        dictionary) / len(col_data) * 100
                    # if the number of uniques is too large then treat is a text
                    if dictionary_lenght_percentage > 10 and len(col_data) > 50 and is_full_text==False:
                        dictionary = []
                        dictionary_available = False
                col_stats = {
                    KEYS.DATA_TYPE: DATA_TYPES.TEXT if is_full_text else data_type,
                    "dictionary": dictionary,
                    "dictionaryAvailable": dictionary_available,
                    "dictionaryLenghtPercentage": dictionary_lenght_percentage,
                    "histogram": histogram
                }
            stats[col_name] = col_stats
            stats[col_name]['data_type_dist'] = data_type_dist
            stats[col_name]['column'] = col_name
            stats[col_name]['empty_cells'] = empty_count[col_name]
            stats[col_name]['empty_percentage'] = empty_count[col_name] * 100 / column_count[col_name]


        for i, col_name in enumerate(all_sampled_data):
            stats[col_name].update(self._compute_duplicates_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_empty_cells_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_clf_based_correlation_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_data_type_dist_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_z_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_lof_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_similariy_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_value_distribution_score(stats, all_sampled_data, col_name))

            stats[col_name].update(self._compute_data_quality_score(stats, all_sampled_data, col_name))


        total_rows = len(self.transaction.input_data.data_array)
        test_rows = len(self.transaction.input_data.test_indexes)
        validation_rows = len(self.transaction.input_data.validation_indexes)
        train_rows = len(self.transaction.input_data.train_indexes)

        self.transaction.persistent_model_metadata.column_stats = stats
        self.transaction.persistent_model_metadata.total_row_count = total_rows
        self.transaction.persistent_model_metadata.test_row_count = test_rows
        self.transaction.persistent_model_metadata.train_row_count = train_rows
        self.transaction.persistent_model_metadata.validation_row_count = validation_rows

        self.transaction.persistent_model_metadata.update()

        self._log_interesting_stats(stats)
        return stats



def test():
    from mindsdb import MindsDB
    mdb = MindsDB()

    # We tell mindsDB what we want to learn and from what data
    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        predict='rental_price',  # the column we want to learn to predict given all the data in the file
        model_name='home_rentals',  # the name of this model
        breakpoint=PHASE_STATS_GENERATOR)

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
