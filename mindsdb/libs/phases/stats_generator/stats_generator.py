import random
import time
import warnings
import imghdr
import sndhdr
import logging
from collections import Counter

import numpy as np
from scipy.stats import entropy
from dateutil.parser import parse as parse_datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import MiniBatchKMeans
import imagehash
from PIL import Image

from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive, clean_float
from mindsdb.libs.helpers.debugging import *
from mindsdb.libs.phases.stats_generator.scores import *
from mindsdb.libs.phases.stats_generator.data_preparation import sample_data, clean_int_and_date_data

class StatsGenerator(BaseModule):
    """
    # The stats generator phase is responsible for generating the insights we need about the data in order to vectorize it
    # Additionally, the stats generator also provides the user with some extra meaningful information about his data,
    though this functionality may be moved to a different step (after vectorization) in the future
    """
    def _get_file_type(self, potential_path):
        could_be_fp = False
        for char in ('/', '\\', ':\\'):
            if char in potential_path:
                could_be_fp = True

        if not could_be_fp:
            return False

        try:
            is_img = imghdr.what(potential_path)
            if is_img is None:
                return False
            else:
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
        Takes in column data and defines if its categorical or full_text

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

        # If all sentences are less than or equal and 3 words, assume it's a category rather than a sentence
        if max_number_of_words <= 3:
            if len(key_count.keys()) < 3:
                return DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.SINGLE
            else:
                return DATA_TYPES.CATEGORICAL, DATA_SUBTYPES.MULTIPLE
        else:
            return DATA_TYPES.SEQUENTIAL, DATA_SUBTYPES.TEXT


    def _get_column_data_type(self, data, data_frame, col_name):
        """
        Provided the column data, define if its numeric, data or class

        :param data: a list containing each of the cells in a column

        :return: type and type distribution, we can later use type_distribution to determine data quality
        NOTE: type distribution is the count that this column has for belonging cells to each DATA_TYPE
        """

        type_dist = {}
        subtype_dist = {}
        additional_info = {'other_potential_subtypes': [], 'other_potential_types': []}

        # calculate type_dist
        if len(data) < 1:
            self.log.warning(f'Column {col_name} has no data in it. Please remove {col_name} from the training file or fill in some of the values !')
            return None, None, None, None, None, 'Column empty'

        for element in data:
            # Maybe use list of functions in the future
            element = str(element)
            current_subtype_guess = 'Unknown'
            current_type_guess = 'Unknown'

            # Check if Nr
            if current_subtype_guess == 'Unknown' or current_type_guess == 'Unknown':
                subtype = self._is_number(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.NUMERIC
                    current_subtype_guess = subtype

            # Check if date
            if current_subtype_guess == 'Unknown' or current_type_guess == 'Unknown':
                subtype = self._get_date_type(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.DATE
                    current_subtype_guess = subtype

            # Check if sequence
            if current_subtype_guess == 'Unknown' or current_type_guess == 'Unknown':
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
            if current_subtype_guess == 'Unknown' or current_type_guess == 'Unknown':
                subtype = self._get_file_type(element)
                if subtype is not False:
                    current_type_guess = DATA_TYPES.FILE_PATH
                    current_subtype_guess = subtype

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

        # assume that the type is the one with the most prevalent type_dist
        for data_type in type_dist:
            # If any of the members are Unknown, use that data type (later to be turned into CATEGORICAL or SEQUENTIAL), since otherwise the model will crash when casting
            # @TODO consider removing or flagging rows where data type is unknown in the future, might just be corrupt data... a bit hard to imply currently
            if data_type == 'Unknown':
                curr_data_type = 'Unknown'
                break
            if type_dist[data_type] > max_data_type:
                curr_data_type = data_type
                max_data_type = type_dist[data_type]

        # If a mix of dates and numbers interpret all as dates
        if DATA_TYPES.DATE in type_dist and len(set(type_dist.keys()) - set([DATA_TYPES.NUMERIC])) == 1:
            if DATA_TYPES.NUMERIC in type_dist:
                type_dist[DATA_TYPES.DATE] += type_dist[DATA_TYPES.NUMERIC]
                del type_dist[DATA_TYPES.NUMERIC]

            if DATA_SUBTYPES.FLOAT in subtype_dist:
                subtype_dist[DATA_SUBTYPES.TIMESTAMP] += subtype_dist[DATA_SUBTYPES.FLOAT]
                del subtype_dist[DATA_SUBTYPES.FLOAT]

            if DATA_SUBTYPES.INT in subtype_dist:
                subtype_dist[DATA_SUBTYPES.TIMESTAMP] += subtype_dist[DATA_SUBTYPES.INT]
                del subtype_dist[DATA_SUBTYPES.INT]

            curr_data_type = DATA_TYPES.DATE

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

        if curr_data_type != DATA_TYPES.CATEGORICAL and curr_data_subtype != DATA_SUBTYPES.DATE:
            all_values = data_frame[col_name]
            all_distinct_vals = set(all_values)

            # The numbers here are picked randomly, the gist of it is that if values repeat themselves a lot we should consider the column to be categorical
            nr_vals = len(all_values)
            nr_distinct_vals = len(all_distinct_vals)

            if ( nr_vals/20 > nr_distinct_vals and (curr_data_type not in [DATA_TYPES.NUMERIC, DATA_TYPES.DATE] or nr_distinct_vals < 20) ) or (curr_data_subtype == DATA_SUBTYPES.TEXT and self.transaction.lmd['handle_text_as_categorical']):
                additional_info['other_potential_subtypes'].append(curr_data_type)
                additional_info['other_potential_types'].append(curr_data_subtype)
                curr_data_type = DATA_TYPES.CATEGORICAL
                if len(all_distinct_vals) < 3:
                    curr_data_subtype = DATA_SUBTYPES.SINGLE
                else:
                    curr_data_subtype = DATA_SUBTYPES.MULTIPLE
                type_dist = {}
                subtype_dist = {}

                type_dist[curr_data_type] = len(data)
                subtype_dist[curr_data_subtype] = len(data)

        if col_name in self.transaction.lmd['force_categorical_encoding']:
            curr_data_type = DATA_TYPES.CATEGORICAL
            curr_data_subtype = DATA_SUBTYPES.MULTIPLE
            type_dist[curr_data_type] = len(data)
            subtype_dist[curr_data_subtype] = len(data)

        return curr_data_type, curr_data_subtype, type_dist, subtype_dist, additional_info, 'Column ok'

    @staticmethod
    def get_words_histogram(data, is_full_text=False):
        """ Returns an array of all the words that appear in the dataset and the number of times each word appears in the dataset """

        splitter = lambda w, t: [wi.split(t) for wi in w] if isinstance(w, list) else splitter(w, t)

        if is_full_text:
            # get all words in every cell and then calculate histograms
            words = []
            for cell in data:
                words += splitRecursive(cell, WORD_SEPARATORS)

            hist = {i: words.count(i) for i in words}
        else:
            hist = {i: data.count(i) for i in data}

        return {
            'x': list(hist.keys()),
            'y': list(hist.values())
        }

    @staticmethod
    def get_histogram(data, data_type=None, data_subtype=None, full_text=None, hmd=None):
        """ Returns a histogram for the data and [optionaly] the percentage buckets"""
        if data_type == DATA_TYPES.SEQUENTIAL:
            is_full_text = True if data_subtype == DATA_SUBTYPES.TEXT else False
            return StatsGenerator.get_words_histogram(data, is_full_text), None
        elif data_type == DATA_TYPES.NUMERIC or data_subtype == DATA_SUBTYPES.TIMESTAMP:
            Y, X = np.histogram(data, bins=min(50,len(set(data))), range=(min(data),max(data)), density=False)
            if data_subtype == DATA_SUBTYPES.INT:
                Y, X = np.histogram(data, bins=[int(round(x)) for x in X], density=False)

            X = X[:-1].tolist()
            Y = Y.tolist()

            return {
                'x': X
                ,'y': Y
            }, X
        elif data_type == DATA_TYPES.CATEGORICAL or data_subtype == DATA_SUBTYPES.DATE :
            histogram = Counter(data)
            X = list(map(str,histogram.keys()))
            Y = list(histogram.values())
            return {
                'x': X,
                'y': Y
            }, Y
        elif data_subtype == DATA_SUBTYPES.IMAGE:
            image_hashes = []
            for img_path in data:
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
                x.append(data.iloc[index_of_most_similar])

            indices = kmeans.predict(image_hashes)
            for index in indices:
                y[index] +=1

            return {
                'x': x,
                'y': y
            }, list(kmeans.cluster_centers_)
        else:
            return None, None

    @staticmethod
    def is_foreign_key(column_name, column_stats, data):
        foregin_key_type = DATA_SUBTYPES.INT in column_stats['other_potential_subtypes'] or DATA_SUBTYPES.INT == column_stats['data_subtype']

        data_looks_like_id = True

        # No need to run this check if the type already indicates a foreign key like value
        if not foregin_key_type:
            val_length = None
            for val in data:
                is_uuid = True
                is_same_length = False

                for char in str(val):
                    if char not in ['0', '1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','-']:
                        is_uuid = False

                if val_length is not False:
                    if val_length is None:
                        val_length = len(str(val))
                    if len(str(val)) == val_length:
                        is_same_length = True

                if not is_uuid and not is_same_length:
                    data_looks_like_id = False
                    break

        foreign_key_name = False
        for endings in ['-id', '_id', 'ID', 'Id']:
            if column_name.endswith(endings):
                foreign_key_name = True
        for keyword in ['account', 'uuid', 'identifier', 'user']:
            if keyword in column_name:
                foreign_key_name = True

        return foreign_key_name and (foregin_key_type or data_looks_like_id)


    def _log_interesting_stats(self, stats):
        """
        # Provide interesting insights about the data to the user and send them to the logging server in order for it to generate charts

        :param stats: The stats extracted up until this point for all columns
        """
        for col_name in stats:
            col_stats = stats[col_name]
            # Overall quality
            if 'quality_score' in col_stats and col_stats['quality_score'] < 6:
                # Some scores are not that useful on their own, so we should only warn users about them if overall quality is bad.
                self.log.warning('Column "{}" is considered of low quality, the scores that influenced this decision will be listed below')
                if 'duplicates_score' in col_stats and col_stats['duplicates_score'] < 6:
                    duplicates_percentage = col_stats['duplicates_percentage']
                    w = f'{duplicates_percentage}% of the values in column {col_name} seem to be repeated, this might indicate that your data is of poor quality.'
                    self.log.warning(w)
                    col_stats['duplicates_score_warning'] = w
                else:
                    col_stats['duplicates_score_warning'] = None
            else:
                col_stats['duplicates_score_warning'] = None

            #Compound scores
            if 'consistency_score' in col_stats and  col_stats['consistency_score'] < 3:
                w = f'The values in column {col_name} rate poorly in terms of consistency. This means that the data has too many empty values, values with a hard to determine type and duplicate values. Please see the detailed logs below for more info'
                self.log.warning(w)
                col_stats['consistency_score_warning'] = w
            else:
                col_stats['consistency_score_warning'] = None

            if 'redundancy_score' in col_stats and  col_stats['redundancy_score'] < 5:
                w = f'The data in the column {col_name} is likely somewhat redundant, any insight it can give us can already by deduced from your other columns. Please see the detailed logs below for more info'
                self.log.warning(w)
                col_stats['redundancy_score_warning'] = w
            else:
                col_stats['redundancy_score_warning'] = None

            if 'variability_score' in col_stats and  col_stats['variability_score'] < 6:
                w = f'The data in the column {col_name} seems to contain too much noise/randomness based on the value variability. That is to say, the data is too unevenly distributed and has too many outliers. Please see the detailed logs below for more info.'
                self.log.warning(w)
                col_stats['variability_score_warning'] = w
            else:
                col_stats['variability_score_warning'] = None

            # Some scores are meaningful on their own, and the user should be warnned if they fall below a certain threshold
            if col_stats['empty_cells_score'] < 8:
                empty_cells_percentage = col_stats['empty_percentage']
                w = f'{empty_cells_percentage}% of the values in column {col_name} are empty, this might indicate that your data is of poor quality.'
                self.log.warning(w)
                col_stats['empty_cells_score_warning'] = w
            else:
                col_stats['empty_cells_score_warning'] = None

            if col_stats['data_type_distribution_score'] < 7:
                percentage_of_data_not_of_principal_type = col_stats['data_type_distribution_score'] * 100
                principal_data_type = col_stats['data_type']
                w = f'{percentage_of_data_not_of_principal_type}% of your data is not of type {principal_data_type}, which was detected to be the data type for column {col_name}, this might indicate that your data is of poor quality.'
                self.log.warning(w)
                col_stats['data_type_distribution_score_warning'] = w
            else:
                col_stats['data_type_distribution_score_warning'] = None

            if 'z_test_based_outlier_score' in col_stats and col_stats['z_test_based_outlier_score'] < 6:
                percentage_of_outliers = col_stats['z_test_based_outlier_score']*100
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data is more than 3 standard deviations away from the mean, this means that there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['z_test_based_outlier_score_warning'] = w
            else:
                col_stats['z_test_based_outlier_score_warning'] = None

            if 'lof_based_outlier_score' in col_stats and col_stats['lof_based_outlier_score'] < 4:
                percentage_of_outliers = col_stats['percentage_of_log_based_outliers']
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data doesn't fit closely in any cluster using the KNN algorithm (20n) to cluster your data, this means that there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['lof_based_outlier_score_warning'] = w
            else:
                col_stats['lof_based_outlier_score_warning'] = None

            if 'value_distribution_score' in col_stats and col_stats['value_distribution_score'] < 3:
                max_probability_key = col_stats['max_probability_key']
                w = f"""Column {col_name} is very biased towards the value {max_probability_key}, please make sure that the data in this column is correct !"""
                self.log.warning(w)
                col_stats['value_distribution_score_warning'] = w
            else:
                col_stats['value_distribution_score_warning'] = None

            if 'similarity_score' in col_stats and col_stats['similarity_score'] < 6:
                similar_percentage = col_stats['max_similarity'] * 100
                similar_col_name = col_stats['most_similar_column_name']
                w = f'Column {col_name} and {similar_col_name} are {similar_percentage}% the same, please make sure these represent two distinct features of your data !'
                self.log.warning(w)
                col_stats['similarity_score_warning'] = w
            else:
                col_stats['similarity_score_warning'] = None

            '''
            if col_stats['correlation_score'] < 5:
                not_quite_correlation_percentage = col_stats['correlation_score'] * 100
                most_correlated_column = col_stats['most_correlated_column']
                self.log.warning(f"""Using a statistical predictor we\'v discovered a correlation of roughly {not_quite_correlation_percentage}% between column
                {col_name} and column {most_correlated_column}""")
            '''

            # We might want to inform the user about a few stats regarding his column regardless of the score, this is done below
            self.log.info(f"""Data distribution for column "{col_name}" of type "{stats[col_name]['data_type']}" and subtype  "{stats[col_name]['data_subtype']}""")
            try:
                self.log.infoChart(stats[col_name]['data_subtype_dist'], type='list', uid='Data Type Distribution for column "{}"'.format(col_name))
            except:
                # Functionality is specific to mindsdb logger
                pass

    def run(self, input_data, hmd=None, print_logs=True):
        """
        # Runs the stats generation phase
        # This shouldn't alter the columns themselves, but rather provide the `stats` metadata object and update the types for each column
        # A lot of information about the data distribution and quality will  also be logged to the server in this phase
        """

        stats = {}
        stats_v2 = {}
        col_data_dict = {}

        if print_logs == False:
            self.log = logging.getLogger('null-logger')
            self.log.propagate = False

        sample_df = sample_data(input_data.data_frame, self.transaction.lmd['sample_margin_of_error'], self.transaction.lmd['sample_confidence_level'], self.log)

        for col_name in self.transaction.lmd['empty_columns']:
            stats_v2[col_name]['empty'] = {'is_empty': True}

        for col_name in sample_df.columns.values:
            stats_v2[col_name] = {}
            stats[col_name] = {}

            len_wo_nulls = len(input_data.data_frame[col_name].dropna())
            len_w_nulls = len(input_data.data_frame[col_name])
            len_unique = len(set(input_data.data_frame[col_name]))
            stats_v2[col_name]['empty'] = {
                'empty_cells': len_w_nulls - len_wo_nulls
                ,'empty_percentage': 100 * round((len_w_nulls - len_wo_nulls)/len_w_nulls,3)
                ,'is_empty': False
            }

            col_data = sample_df[col_name].dropna()

            data_type, data_subtype, data_type_dist, data_subtype_dist, additional_info, column_status = self._get_column_data_type(col_data, input_data.data_frame, col_name)

            stats_v2[col_name]['typing'] = {
                'data_type': data_type
                ,'data_subtype': data_subtype
                ,'data_type_dist': data_type_dist
                ,'data_subtype_dist': data_subtype_dist
            }

            for k  in stats_v2[col_name]['typing']: stats[col_name][k] = stats_v2[col_name]['typing'][k]

            # Do some temporary processing for timestamp and numerical values
            if data_type == DATA_TYPES.NUMERIC or data_subtype == DATA_SUBTYPES.TIMESTAMP:
                col_data = clean_int_and_date_data(col_data, self.log)

            hist_data = col_data
            if data_type == DATA_TYPES.CATEGORICAL:
                hist_data = input_data.data_frame[col_name]
                stats_v2[col_name]['unique'] = {
                    'unique_values': len_unique
                    ,'unique_percentage': 100 * round((len_w_nulls - len_unique)/len_w_nulls,8)
                }

            histogram, percentage_buckets = StatsGenerator.get_histogram(hist_data, data_type=data_type, data_subtype=data_subtype)

            stats[col_name]['histogram'] = histogram
            stats[col_name]['percentage_buckets'] = percentage_buckets
            stats_v2[col_name]['histogram'] = histogram
            stats_v2[col_name]['percentage_buckets'] = percentage_buckets

            stats[col_name]['empty_cells'] = stats_v2[col_name]['empty']['empty_cells']
            stats[col_name]['empty_percentage'] = stats_v2[col_name]['empty']['empty_percentage']

            stats_v2[col_name]['additional_info'] = additional_info
            for k in additional_info:
                stats[col_name][k] = additional_info[k]

            col_data_dict[col_name] = col_data

        for col_name in sample_df.columns:
            data_type = stats_v2[col_name]['typing']['data_type']
            data_subtype = stats_v2[col_name]['typing']['data_subtype']

            # For now there's only one and computing it takes way too long, so this is not enabled
            scores = []

            for score_promise in scores:
                # Wait for function on process to finish running
                score = score_promise.get()
                stats[col_name].update(score)

            for score_func in [compute_duplicates_score, compute_empty_cells_score, compute_data_type_dist_score, compute_z_score, compute_lof_score, compute_similariy_score, compute_value_distribution_score]:
                start_time = time.time()

                try:
                    if 'compute_z_score' in str(score_func) or 'compute_lof_score' in str(score_func):
                        stats[col_name].update(score_func(stats, col_data_dict, col_name))
                    else:
                        stats[col_name].update(score_func(stats, sample_df, col_name))
                except Exception as e:
                    self.log.warning(e)

                fun_name = str(score_func)
                run_duration = round(time.time() - start_time, 2)

            for score_func in [compute_consistency_score, compute_redundancy_score, compute_variability_score, compute_data_quality_score]:
                try:
                    stats[col_name].update(score_func(stats, col_name))
                except Exception as e:
                    self.log.warning(e)

            stats[col_name]['is_foreign_key'] = self.is_foreign_key(col_name, stats[col_name], col_data_dict[col_name])
            if stats[col_name]['is_foreign_key'] and self.transaction.lmd['handle_foreign_keys']:
                self.transaction.lmd['columns_to_ignore'].append(col_name)

            # New logic
            col_data = sample_df[col_name]

            if data_type in (DATA_TYPES.NUMERIC,DATA_TYPES.DATE,DATA_TYPES.CATEGORICAL) or data_subtype in (DATA_SUBTYPES.IMAGE):
                nr_values = sum(stats_v2[col_name]['histogram']['y'])
                S = entropy([x/nr_values for x in stats_v2[col_name]['histogram']['y']],base=max(2,len(stats_v2[col_name]['histogram']['y'])))
                stats_v2[col_name]['bias'] = {
                    'entropy': S
                }
                if S < 0.25:
                    pick_nr = -max(1, int(len(stats_v2[col_name]['histogram']['y'])/10))
                    stats_v2[col_name]['bias']['biased_buckets'] = [stats_v2[col_name]['histogram']['x'][i] for i in np.array(stats_v2[col_name]['histogram']['y']).argsort()[pick_nr:]]

            if 'lof_outliers' in stats[col_name]:
                if data_subtype in (DATA_SUBTYPES.INT):
                    stats[col_name]['lof_outliers'] = [int(x) for x in stats[col_name]['lof_outliers']]

                stats_v2[col_name]['outliers'] = {
                    'outlier_values': stats[col_name]['lof_outliers']
                    ,'outlier_score': stats[col_name]['lof_based_outlier_score']
                }

        self.transaction.lmd['column_stats'] = stats
        self.transaction.lmd['stats_v2'] = stats_v2

        self.transaction.lmd['data_preparation']['accepted_margin_of_error'] = self.transaction.lmd['sample_margin_of_error']

        self.transaction.lmd['data_preparation']['total_row_count'] = len(input_data.data_frame)
        self.transaction.lmd['data_preparation']['used_row_count'] = len(sample_df)

        self._log_interesting_stats(stats)
