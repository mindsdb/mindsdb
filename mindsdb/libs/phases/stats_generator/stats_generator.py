import random
import warnings
import imghdr
import sndhdr
from collections import Counter

import numpy as np
import scipy.stats as st
from dateutil.parser import parse as parse_datetime
from sklearn.neighbors import LocalOutlierFactor
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import matthews_corrcoef
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import MiniBatchKMeans
import imagehash
from PIL import Image

from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive, clean_float, cast_string_to_python_type
from mindsdb.external_libs.stats import calculate_sample_size



class StatsGenerator(BaseModule):
    """
    # The stats generator phase is responsible for generating the insights we need about the data in order to vectorize it
    # Additionally, the stats generator also provides the user with some extra meaningful information about his data,
    thoguh this functionality may be moved to a different step (after vectorization) in the future
    """

    phase_name = PHASE_STATS_GENERATOR

    def _get_file_type(self, potential_path):
        could_be_fp = False
        for char in ('/', '\\', ':\\'):
            if char in potential_path:
                could_be_fp = True

        if not could_be_fp:
            return False

        try:
            is_img = imghdr.what(potential_path)
            if is_img is not None:
                return DATA_SUBTYPES.IMAGE
        except:
            # Not a file or file doesn't exist
            return False

        # @TODO: CURRENTLY DOESN'T DIFFERENTIATE BETWEEN AUDIO AND VIDEO
        is_audio = sndhdr.what(potential_path)
        if is_audio is not None:
            return DATA_SUBTYPES.AUDIO

        return False

    def _is_number(self, string):
        """ Returns True if string is a number. """
        try:
            # Should crash if not number
            clean_float(str(string))
            if '.' in str(string) or ',' in str(string):
                return DATA_SUBTYPES.FLOAT
            else:
                return DATA_SUBTYPES.INT
        except ValueError:
            return False

    def _get_date_type(self, string):
        """ Returns True if string is a valid date format """
        try:
            dt = parse_datetime(string)

            # Not accurate 100% for a single datetime str, but should work in aggregate
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and len(string) <= 16:
                return DATA_SUBTYPES.DATE
            else:
                return DATA_SUBTYPES.TIMESTAMP
        except:
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
            return DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.SINGLE
        if max_number_of_words <= 3 and len(key_count) < total_length * 0.8:
            # @TODO This used to be multiple... but, makes no sense for cateogry, should be discussed
            return DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.SINGLE
        else:
            return DATA_TYPES.SEQUENTIAL, DATA_SUBTYPES.TEXT


    def _get_column_data_type(self, data, col_index, data_array, col_name):
        """
        Provided the column data, define it its numeric, data or class

        :param data: a list containing each of the cells in a column

        :return: type and type distribution, we can later use type_distribution to determine data quality
        NOTE: type distribution is the count that this column has for belonging cells to each DATA_TYPE
        """

        type_dist = {}
        subtype_dist = {}
        additional_info = {}

        # calculate type_dist
        if len(data) < 1:
            self.log.warning(f'Column {col_name} has not data in it. Please remove {col_name} from the training file or fill in some of the values !')
            return None, None, None, None, None, 'Column empty'

        for element in data:
            # Maybe use list of functions in the future
            element = element
            current_subtype_guess = 'Unknown'
            current_type_guess = 'Unknown'

            # Check if Nr
            if current_subtype_guess is 'Unknown' or current_type_guess is 'Unknown':
                subtype = self._is_number(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.NUMERIC
                    current_subtype_guess = subtype

            # Check if date
            if current_subtype_guess is 'Unknown' or current_type_guess is 'Unknown':
                subtype = self._get_date_type(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.DATE
                    current_subtype_guess = subtype

            # Check if sequence
            if current_subtype_guess is 'Unknown' or current_type_guess is 'Unknown':
                for char in [',','\t','|',' ']:
                    try:
                        all_nr = True
                        eles = element.rstrip(']').lstrip('[').split(char)
                        for ele in eles:
                            if not self._is_number(ele):
                                all_nr = False
                    except:
                        all_nr = False
                        pass
                    if all_nr is True:
                        additional_info['separator'] = char
                        current_type_guess = DATA_TYPES.SEQUENTIAL
                        current_subtype_guess = DATA_SUBTYPES.ARRAY
                        break

            # Check if file
            if current_subtype_guess is 'Unknown' or current_type_guess is 'Unknown':
                subtype = self._get_file_type(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.FILE_PATH
                    current_subtype_guess = subtype

            # If nothing works, assume it's categorical or sequential and determine type later (based on all the data in the column)

            if current_type_guess not in type_dist:
                type_dist[current_type_guess] = 1
            else:
                type_dist[current_type_guess] += 1

            if current_subtype_guess not in subtype_dist:
                subtype_dist[current_subtype_guess] = 1
            else:
                subtype_dist[current_subtype_guess] += 1

        curr_data_type = 'Unknown'
        curr_data_subtype = 'Unknown'
        max_data_type = 0

        # assume that the type is the one with the most prevelant type_dist
        for data_type in type_dist:
            # If any of the members are Unknown, use that data type (later to be turned into CATEGORICAL or SEQUENTIAL), since otherwise the model will crash when casting
            # @TODO consider removing rows where data type is unknown in the future, might just be corrupt data... a bit hard to impl currently
            if data_type == 'Unknown':
                curr_data_type = 'Unknown'
                break
            if type_dist[data_type] > max_data_type:
                curr_data_type = data_type
                max_data_type = type_dist[data_type]

        # Set subtype
        max_data_subtype = 0
        if curr_data_type != 'Unknown':
            for data_subtype in subtype_dist:
                if subtype_dist[data_subtype] > max_data_subtype and data_subtype in DATA_TYPES_SUBTYPES.subtypes[curr_data_type]:
                    curr_data_subtype = data_subtype
                    max_data_subtype = subtype_dist[data_subtype]

        # If it finds that the type is categorical it should determine if its categorical or actual text
        if curr_data_type == 'Unknown':
            curr_data_type, curr_data_subtype = self._get_text_type(data)
            type_dist[curr_data_type] = type_dist.pop('Unknown')
            subtype_dist[curr_data_subtype] = subtype_dist.pop('Unknown')

        all_values = []
        for row in data_array:
            all_values.append(row[col_index])

        all_distinct_vals = set(all_values)

        # Let's chose so random number
        if (len(all_distinct_vals) < len(all_values)/200) or ( (len(all_distinct_vals) < 120) and (len(all_distinct_vals) < len(all_values)/6) ):
            curr_data_type = DATA_TYPES.CATEGORICAL
            if len(all_distinct_vals) < 3:
                curr_data_subtype = DATA_SUBTYPES.SINGLE
            else:
                curr_data_subtype = DATA_SUBTYPES.MULTIPLE
            type_dist = {}
            subtype_dist = {}

            type_dist[curr_data_type] = len(data)
            subtype_dist[curr_data_subtype] = len(data)

        return curr_data_type, curr_data_subtype, type_dist, subtype_dist, additional_info, 'Column ok'

    def _get_words_dictionary(self, data, full_text = False):
        """ Returns an array of all the words that appear in the dataset and the number of times each word appears in the dataset """

        splitter = lambda w, t: [wi.split(t) for wi in w] if type(w) == type([]) else splitter(w,t)

        if full_text:
            # get all words in every cell and then calculate histograms
            words = []
            for cell in data:
                words += splitRecursive(cell, WORD_SEPARATORS)

            hist = {i: words.count(i) for i in words}
        else:
            hist = {i: data.count(i) for i in data}

        x = list(hist.keys())
        histogram = {
            'x': x,
            'y': list(hist.values())
        }
        return x, histogram

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
            ,'value_distribution_score_description': """
            This score can indicate either biasing towards one specific value in the column or a large number of outliers. So it is a reliable quality indicator but we can't know for which of the two reasons.
            """
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
            duplicates_score: a quality based on the duplicate percentage, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
        """

        occurances = Counter(columns[col_name])
        values_that_occur_twice_or_more = filter(lambda val: occurances[val] < 2, occurances)
        nr_of_occurances = map(lambda val: occurances[val], values_that_occur_twice_or_more)
        nr_duplicates = sum(nr_of_occurances)
        data = {
            'nr_duplicates': nr_duplicates
            ,'duplicates_percentage': nr_duplicates*100/len(columns[col_name])
            ,'duplicates_score_description':"""
            The duplicates score consists in the % of duplicate values / 100. So, it can range from 0 (no duplicates) to 1 (all the values have one or more duplicates). This score being large, on it's own, is not necessarily an indicator that your data is of poor quality.
            """
        }

        if stats[col_name]['data_type'] != DATA_TYPES.CATEGORICAL and stats[col_name]['data_type'] != DATA_TYPES.DATE:
            data['duplicates_score'] = data['duplicates_percentage']/100
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

        return {'empty_cells_score': stats[col_name]['empty_percentage']/100
                ,'empty_cells_score_description':"""This score is computed as the % of empty values / 100. Empty values in a column are always bad for training correctly on that data."""}

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
        return {'data_type_distribution_score': data_type_dist_score
        ,'data_type_distribution_score_description':"""
        This score indicates the amount of data that are not of the same data type as the most commonly detected data type in this column. Note, the most commonly occuring data type is not necessarily the type mindsdb will use to label the column when learning or predicting.
        """}

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
        if stats[col_name]['data_type'] != DATA_TYPES.NUMERIC:
            return {}

        z_scores = list(map(abs,(st.zscore(columns[col_name]))))
        threshold = 3
        z_score_outlier_indexes = [i for i in range(len(z_scores)) if z_scores[i] > threshold]
        data = {
            'z_score_outliers': z_score_outlier_indexes
            ,'mean_z_score': np.mean(z_scores)
            ,'z_test_based_outlier_score': len(z_score_outlier_indexes)/len(columns[col_name])
            ,'z_test_based_outlier_score_description':"""
            This score indicates the amount of data that are 3 STDs or more away from the mean. That is to say, the amount of data that we consider to be an outlir. A hgih z socre means your data contains a large amount of outliers.
            """
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

        if stats[col_name]['data_type'] != DATA_TYPES.NUMERIC:
            return {}

        np_col_data = np.array(columns[col_name]).reshape(-1, 1)
        lof = LocalOutlierFactor(contamination='auto')
        outlier_scores = lof.fit_predict(np_col_data)
        outlier_indexes = [i for i in range(len(columns[col_name])) if outlier_scores[i] < -0.8]

        return {
            'lof_outliers': outlier_indexes
            ,'lof_based_outlier_score': len(outlier_indexes)/len(columns[col_name])
            ,'percentage_of_log_based_outliers': (len(outlier_indexes)/len(columns[col_name])) * 100
            ,'lof_based_outlier_score_description':"""
            The higher this score, the more outliers your dataset has. This is based on distance from the center of 20 clusters as constructed via KNN.
            """
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

        if len(similarities) > 0:
            max_similarity = max(map(lambda x: x[1], similarities))
            most_similar_column_name = list(filter(lambda x: x[1] == max_similarity, similarities))[0][0]
        else:
            max_similarity = 0
            most_similar_column_name = None

        return {
            'similarities': similarities
            ,'similarity_score': max_similarity
            ,'most_similar_column_name': most_similar_column_name
            ,'similarity_score_description':"""
            This score is simply a matthews correlation applied between this column and all other column.
            The score * 100 is the number of values which are similar in the column that is most similar to the scored column.
            """
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
            ,'similarity_score_description':"""
            A high value for this score means that two of your columns are highly similar. This is done by trying to predict one column using the other via a simple DT.
            """
        }

    def _compute_consistency_score(self, stats, col_name):
        """
        # Attempts to determine the consistency of the data in a column
        by taking into account the ty[e distribution, nr of empty cells and duplicates

        :param stats: The stats extracted up until this point for all columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            consistency_score: The socre, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality
        """
        col_stats = stats[col_name]
        if 'duplicates_score' in col_stats:
            consistency_score = (col_stats['data_type_distribution_score'] + col_stats['empty_cells_score'])/2.5 + col_stats['duplicates_score']/5
        else:
            consistency_score = (col_stats['data_type_distribution_score'] + col_stats['empty_cells_score'])/2
        return {'consistency_score': consistency_score
        ,'consistency_score_description':"""
        A high value for this score indicates that the data in a column is not very consistent, it's either missing a lot of valus or the type of values it has varries quite a lot (e.g. combination of strings, dates, integers and floats).
        The data consistency score is mainly based upon the Data Type Distribution Score and the Empty Cells Score, the Duplicates Score is also taken into account if present but with a smaller (2x smaller) bias.
        """}

    def _compute_redundancy_score(self, stats, col_name):
        """
        # Attempts to determine the redundancy of the column by taking into account correlation and
        similarity with other columns

        :param stats: The stats extracted up until this point for all columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            consistency_score: The socre, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality
        """
        col_stats = stats[col_name]
        redundancy_score = (col_stats['similarity_score'])/1
        return {'redundancy_score': redundancy_score
            ,'redundancy_score_description':"""
            A high value in this score indicates the data in this column is highly redundant for making any sort of prediction, you should make sure that values heavily related to this column are no already expressed in another column (e.g. if this column is a timestamp, make sure you don't have another column representing the exact same time in ISO datetime format).
            The value is based in equal part on the Similarity Score and the Correlation Score.
            """}

    def _compute_variability_score(self, stats, col_name):
        """
        # Attempts to determine the variability/randomness of a column by taking into account
        the z and lof outlier scores and the value distribution score (histogram biasing towards a few buckets)

        :param stats: The stats extracted up until this point for all columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            consistency_score: The socre, ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality
        """
        col_stats = stats[col_name]
        if 'lof_based_outlier_score' in col_stats and 'z_test_based_outlier_score' in col_stats:
            variability_score = (col_stats['z_test_based_outlier_score'] + col_stats['lof_based_outlier_score']
             + col_stats['value_distribution_score'])/3
        else:
            variability_score = col_stats['value_distribution_score']/2

        return {'variability_score': variability_score
        ,'variability_score_description':"""
        A high value for this score indicates the data in this column seems to be very variable, indicating a large possibility of some random noise affecting your data. This could mean that the values for this column are not collected or processed correctly.
        The value is based in equal part on the Z Test based outliers score, the LOG based outlier score and the Value Distribution Score.
        """}


    def _compute_data_quality_score(self, stats, col_name):
        """
        # Attempts to determine the quality of the column through aggregating all quality score
        we could compute about it

        :param stats: The stats extracted up until this point for all columns
        :param col_name: The name of the column we should compute the new stats for
        :return: Dictioanry containing:
            quality_score: An aggreagted quality socre that attempts to asses the overall quality of the column,
                , ranges from 1 to 0, where 1 is lowest quality and 0 is highest quality.
            bad_scores: The socres which lead to use rating this column poorly
        """

        col_stats = stats[col_name]
        scores = ['consistency_score', 'redundancy_score', 'variability_score']
        quality_score = 0
        for score in scores:
            quality_score += col_stats[score]
        quality_score = quality_score/len(scores)

        return {'quality_score': quality_score
        ,'quality_score_description':"""
        The higher this score is, the lower the quality of a given column.
        """}

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
                self.log.warning('Column "{}" is considered of low quality, the scores that influenced this decission will be listed bellow')
                if 'duplicates_score' in col_stats and col_stats['duplicates_score'] > 0.5:
                    duplicates_percentage = col_stats['duplicates_percentage']
                    w = f'{duplicates_percentage}% of the values in column {col_name} seem to be repeated, this might indicate your data is of poor quality.'
                    self.log.warning(w)
                    col_stats['duplicates_score_warning'] = w
                else:
                    col_stats['duplicates_score_warning'] = None
            else:
                col_stats['duplicates_score_warning'] = None

            #Compound scores
            if col_stats['consistency_score'] > 0.25:
                w = f'The values in column {col_name} rate poorly in terms of consistency. This means the data has too many empty values, values with a hard to determine type and duplicate values. Please see the detailed logs bellow for more info'
                self.log.warning(w)
                col_stats['consistency_score_warning'] = w
            else:
                col_stats['consistency_score_warning'] = None

            if col_stats['redundancy_score'] > 0.45:
                w = f'The data in the column {col_name} is likely somewhat redundant, any insight it can give us can already by deduced from your other columns. Please see the detailed logs bellow for more info'
                self.log.warning(w)
                col_stats['redundancy_score_warning'] = w
            else:
                col_stats['redundancy_score_warning'] = None

            if col_stats['variability_score'] > 0.5:
                w = f'The data in the column {col_name} seems to have too contain too much noise/randomness based on the value variability. That is too say, the data is too unevenly distributed and has too many outliers. Please see the detailed logs bellow for more info.'
                self.log.warning(w)
                col_stats['variability_score_warning'] = w
            else:
                col_stats['variability_score_warning'] = None

            # Some scores are meaningful on their own, and the user should be warnned if they fall bellow a certain threshold
            if col_stats['empty_cells_score'] > 0.2:
                empty_cells_percentage = col_stats['empty_percentage']
                w = f'{empty_cells_percentage}% of the values in column {col_name} are empty, this might indicate your data is of poor quality.'
                self.log.warning(w)
                col_stats['empty_cells_score_warning'] = w
            else:
                col_stats['empty_cells_score_warning'] = None

            if col_stats['data_type_distribution_score'] > 0.2:
                #self.log.infoChart(stats[col_name]['data_type_dist'], type='list', uid='Dubious Data Type Distribution for column "{}"'.format(col_name))
                percentage_of_data_not_of_principal_type = col_stats['data_type_distribution_score'] * 100
                principal_data_type = col_stats['data_type']
                w = f'{percentage_of_data_not_of_principal_type}% of your data is not of type {principal_data_type}, which was detected to be the data type for column {col_name}, this might indicate your data is of poor quality.'
                self.log.warning(w)
                col_stats['data_type_distribution_score_warning'] = w
            else:
                col_stats['data_type_distribution_score_warning'] = None

            if 'z_test_based_outlier_score' in col_stats and col_stats['z_test_based_outlier_score'] > 0.3:
                percentage_of_outliers = col_stats['z_test_based_outlier_score']*100
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data is more than 3 standard deviations away from the mean, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['z_test_based_outlier_score_warning'] = w
            else:
                col_stats['z_test_based_outlier_score_warning'] = None

            if 'lof_based_outlier_score' in col_stats and col_stats['lof_based_outlier_score'] > 0.3:
                percentage_of_outliers = col_stats['percentage_of_log_based_outliers']
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data doesn't fit closely in any cluster using the KNN algorithm (20n) to cluster your data, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['lof_based_outlier_score_warning'] = w
            else:
                col_stats['lof_based_outlier_score_warning'] = None

            if col_stats['value_distribution_score'] > 0.8:
                max_probability_key = col_stats['max_probability_key']
                w = f"""Column {col_name} is very biased towards the value {max_probability_key}, please make sure that the data in this column is correct !"""
                self.log.warning(w)
                col_stats['value_distribution_score_warning'] = w
            else:
                col_stats['value_distribution_score_warning'] = None

            if col_stats['similarity_score'] > 0.5:
                similar_percentage = col_stats['similarity_score'] * 100
                similar_col_name = col_stats['most_similar_column_name']
                w = f'Column {col_name} and {similar_col_name} are {similar_percentage}% the same, please make sure these represent two distinct features of your data !'
                self.log.warning(w)
                col_stats['similarity_score_warning'] = w
            else:
                col_stats['similarity_score_warning'] = None

            '''
            if col_stats['correlation_score'] > 0.4:
                not_quite_correlation_percentage = col_stats['correlation_score'] * 100
                most_correlated_column = col_stats['most_correlated_column']
                self.log.warning(f"""Using a statistical predictor we\'v discovered a correlation of roughly {not_quite_correlation_percentage}% between column
                {col_name} and column {most_correlated_column}""")
            '''

            # We might want to inform the user about a few stats regarding his column regardless of the score, this is done bellow
            self.log.info('Data distribution for column "{}"'.format(col_name))
            self.log.infoChart(stats[col_name]['data_subtype_dist'], type='list', uid='Data Type Distribution for column "{}"'.format(col_name))


    def run(self, input_data, modify_light_metadata, hmd=None):
        """
        # Runs the stats generation phase
        # This shouldn't alter the columns themselves, but rather provide the `stats` metadata object and update the types for each column
        # A lot of information about the data distribution and quality will  also be logged to the server in this phase
        """
        header = input_data.columns
        non_null_data = {}
        all_sampled_data = {}

        for column in header:
            non_null_data[column] = []
            all_sampled_data[column] = []

        empty_count = {}
        column_count = {}

        # we dont need to generate statistic over all of the data, so we subsample, based on our accepted margin of error
        population_size = len(input_data.data_array)

        if population_size < 50:
            sample_size = population_size
        else:
            sample_size = int(calculate_sample_size(population_size=population_size, margin_error=CONFIG.DEFAULT_MARGIN_OF_ERROR, confidence_level=CONFIG.DEFAULT_CONFIDENCE_LEVEL))
            if sample_size > 3000 and sample_size > population_size/8:
                sample_size = min(round(population_size/8),3000)

        # get the indexes of randomly selected rows given the population size
        input_data_sample_indexes = random.sample(range(population_size), sample_size)
        self.log.info('population_size={population_size},  sample_size={sample_size}  {percent:.2f}%'.format(population_size=population_size, sample_size=sample_size, percent=(sample_size/population_size)*100))

        for sample_i in input_data_sample_indexes:
            row = input_data.data_array[sample_i]
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

        col_data_dict = {}
        for i, col_name in enumerate(non_null_data):
            col_data = non_null_data[col_name]
            full_col_data = all_sampled_data[col_name]
            data_type, curr_data_subtype, data_type_dist, data_subtype_dist, additional_info, column_status = self._get_column_data_type(col_data, i, input_data.data_array, col_name)

            if column_status == 'Column empty':
                if modify_light_metadata:
                    self.transaction.lmd['malformed_columns']['names'].append(col_name)
                    self.transaction.lmd['malformed_columns']['indices'].append(i)
                continue

            if data_type == DATA_TYPES.DATE:
                for i, element in enumerate(col_data):
                    if str(element) in [str(''), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null']:
                        col_data[i] = None
                    else:
                        try:
                            col_data[i] = int(parse_datetime(element).timestamp())
                        except:
                            self.log.warning('Could not convert string to date and it was expected, current value {value}'.format(value=element))
                            col_data[i] = None

            if data_type == DATA_TYPES.NUMERIC or data_type == DATA_TYPES.DATE:
                newData = []

                for value in col_data:
                    if value != '' and value != '\r' and value != '\n':
                        newData.append(value)


                col_data = [clean_float(i) for i in newData if str(i) not in ['', str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null']]

                # This shouldn't happen... an all null column makes no sense
                if len(col_data) < 1:
                    return None

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


                    inc_rate = 0.1
                    initial_step_size = abs(max_value-min_value)/100

                    xp += [min_value]
                    i = min_value + initial_step_size

                    while i < max_value:

                        xp += [i]
                        i_inc = abs(i-min_value)*inc_rate
                        i = i + i_inc
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
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
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
            elif data_type == DATA_TYPES.CATEGORICAL:
                all_values = []
                for row in input_data.data_array:
                    all_values.append(row[i])

                histogram = Counter(all_values)
                all_possible_values = histogram.keys()

                col_stats = {
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
                    "histogram": {
                        "x": list(histogram.keys()),
                        "y": list(histogram.values())
                    }
                    #"percentage_buckets": list(histogram.keys())
                }

            elif curr_data_subtype == DATA_SUBTYPES.IMAGE:
                image_hashes = []
                for img_path in col_data:
                    img_hash = imagehash.phash(Image.open(img_path))
                    seq_hash = []
                    for hash_row in img_hash.hash:
                        seq_hash.extend(hash_row)

                    image_hashes.append(np.array(seq_hash))

                kmeans = MiniBatchKMeans(n_clusters=20, batch_size=round(len(image_hashes)/4))

                kmeans.fit(image_hashes)

                if hmd is not None:
                    hmd['bucketing_algorithms'][col_name] = kmeans

                x = []
                y = [0] * len(kmeans.cluster_centers_)

                for cluster in kmeans.cluster_centers_:
                    similarities = cosine_similarity(image_hashes,kmeans.cluster_centers_)

                    similarities = list(map(lambda x: sum(x), similarities))

                    index_of_most_similar = similarities.index(max(similarities))
                    x.append(col_data[index_of_most_similar])

                indices = kmeans.predict(image_hashes)
                for index in indices:
                    y[index] +=1

                col_stats = {
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
                    'percentage_buckets': kmeans.cluster_centers_,
                    'histogram': {
                        'x': x,
                        'y': y
                    }
                }

            # @TODO This is probably wrong, look into it a bit later
            else:
                # see if its a sentence or a word
                is_full_text = True if curr_data_subtype == DATA_SUBTYPES.TEXT else False
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
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
                    "dictionary": dictionary,
                    "dictionaryAvailable": dictionary_available,
                    "dictionaryLenghtPercentage": dictionary_lenght_percentage,
                    "histogram": histogram
                }
            stats[col_name] = col_stats
            stats[col_name]['data_type_dist'] = data_type_dist
            stats[col_name]['data_subtype_dist'] = data_subtype_dist
            stats[col_name]['column'] = col_name
            stats[col_name]['empty_cells'] = empty_count[col_name]
            stats[col_name]['empty_percentage'] = empty_count[col_name] * 100 / column_count[col_name]
            if 'separator' in additional_info:
                stats[col_name]['separator'] = additional_info['separator']
            col_data_dict[col_name] = col_data

        for i, col_name in enumerate(all_sampled_data):
            if col_name in self.transaction.lmd['malformed_columns']['names']:
                continue

            stats[col_name].update(self._compute_duplicates_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_empty_cells_score(stats, all_sampled_data, col_name))
            #stats[col_name].update(self._compute_clf_based_correlation_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_data_type_dist_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_z_score(stats, col_data_dict, col_name))
            stats[col_name].update(self._compute_lof_score(stats, col_data_dict, col_name))
            stats[col_name].update(self._compute_similariy_score(stats, all_sampled_data, col_name))
            stats[col_name].update(self._compute_value_distribution_score(stats, all_sampled_data, col_name))

            stats[col_name].update(self._compute_consistency_score(stats, col_name))
            stats[col_name].update(self._compute_redundancy_score(stats, col_name))
            stats[col_name].update(self._compute_variability_score(stats, col_name))

            stats[col_name].update(self._compute_data_quality_score(stats, col_name))


        total_rows = len(input_data.data_array)

        if modify_light_metadata:
            self.transaction.lmd['column_stats'] = stats
            self.transaction.lmd['data_preparation']['total_row_count'] = total_rows
            self.transaction.lmd['data_preparation']['test_row_count'] = len(input_data.test_indexes)
            self.transaction.lmd['data_preparation']['train_row_count'] = len(input_data.train_indexes)
            self.transaction.lmd['data_preparation']['validation_row_count'] = len(input_data.validation_indexes)

        self._log_interesting_stats(stats)
        return stats



def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    CONFIG.DEBUG_BREAK_POINT = PHASE_STATS_GENERATOR

    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02
    )





# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
