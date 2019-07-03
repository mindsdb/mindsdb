import random
import time
import warnings
import imghdr
import sndhdr
import logging
from collections import Counter
import multiprocessing

import numpy as np
import scipy.stats as st
from dateutil.parser import parse as parse_datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import MiniBatchKMeans
import imagehash
from PIL import Image

from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive, clean_float, cast_string_to_python_type
from mindsdb.external_libs.stats import calculate_sample_size
from mindsdb.libs.phases.stats_generator.scores import *



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

        # @TODO: Extremely slow for large datasets, make it faster
        if curr_data_type != DATA_TYPES.CATEGORICAL and curr_data_subtype != DATA_SUBTYPES.DATE:
            all_values = data_frame[col_name]
            all_distinct_vals = set(all_values)

            # The numbers here are picked randomly, the gist of it is that if values repeat themselves a lot we should consider the column to be categorical
            nr_vals = len(all_values)
            nr_distinct_vals = len(all_distinct_vals)
            if nr_vals/20 > nr_distinct_vals:
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

    @staticmethod
    def clean_int_and_date_data(col_data):
        cleaned_data = []

        for value in col_data:
            if value != '' and value != '\r' and value != '\n':
                cleaned_data.append(value)

        cleaned_data = [clean_float(i) for i in cleaned_data if str(i) not in ['', str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null']]
        return cleaned_data

    @staticmethod
    def get_words_histogram(data, is_full_text=False):
        """ Returns an array of all the words that appear in the dataset and the number of times each word appears in the dataset """

        splitter = lambda w, t: [wi.split(t) for wi in w] if type(w) == type([]) else splitter(w,t)

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
            data = StatsGenerator.clean_int_and_date_data(data)
            y, x = np.histogram(data, 50, density=False)
            x = (x + np.roll(x, -1))[:-1] / 2.0
            x = x.tolist()
            y = y.tolist()
            return {
                'x': x
                ,'y': y
            }, None
        elif data_type == DATA_TYPES.CATEGORICAL or data_subtype == DATA_SUBTYPES.DATE :
            histogram = Counter(data)
            return {
                'x': list(histogram.keys()),
                'y': list(histogram.values())
            }, None
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
                x.append(data[index_of_most_similar])

            indices = kmeans.predict(image_hashes)
            for index in indices:
                y[index] +=1

            return {
                'x': x,
                'y': y
            }, kmeans.cluster_centers_
        else:
            return None, None

    def _log_interesting_stats(self, stats):
        """
        # Provide interesting insights about the data to the user and send them to the logging server in order for it to generate charts

        :param stats: The stats extracted up until this point for all columns
        """
        for col_name in stats:
            col_stats = stats[col_name]
            # Overall quality
            if col_stats['quality_score'] < 6:
                # Some scores are not that useful on their own, so we should only warn users about them if overall quality is bad.
                self.log.warning('Column "{}" is considered of low quality, the scores that influenced this decission will be listed bellow')
                if 'duplicates_score' in col_stats and col_stats['duplicates_score'] < 6:
                    duplicates_percentage = col_stats['duplicates_percentage']
                    w = f'{duplicates_percentage}% of the values in column {col_name} seem to be repeated, this might indicate your data is of poor quality.'
                    self.log.warning(w)
                    col_stats['duplicates_score_warning'] = w
                else:
                    col_stats['duplicates_score_warning'] = None
            else:
                col_stats['duplicates_score_warning'] = None

            #Compound scores
            if col_stats['consistency_score'] < 3:
                w = f'The values in column {col_name} rate poorly in terms of consistency. This means the data has too many empty values, values with a hard to determine type and duplicate values. Please see the detailed logs bellow for more info'
                self.log.warning(w)
                col_stats['consistency_score_warning'] = w
            else:
                col_stats['consistency_score_warning'] = None

            if col_stats['redundancy_score'] < 5:
                w = f'The data in the column {col_name} is likely somewhat redundant, any insight it can give us can already by deduced from your other columns. Please see the detailed logs bellow for more info'
                self.log.warning(w)
                col_stats['redundancy_score_warning'] = w
            else:
                col_stats['redundancy_score_warning'] = None

            if col_stats['variability_score'] < 6:
                w = f'The data in the column {col_name} seems to have too contain too much noise/randomness based on the value variability. That is too say, the data is too unevenly distributed and has too many outliers. Please see the detailed logs bellow for more info.'
                self.log.warning(w)
                col_stats['variability_score_warning'] = w
            else:
                col_stats['variability_score_warning'] = None

            # Some scores are meaningful on their own, and the user should be warnned if they fall bellow a certain threshold
            if col_stats['empty_cells_score'] < 3:
                empty_cells_percentage = col_stats['empty_percentage']
                w = f'{empty_cells_percentage}% of the values in column {col_name} are empty, this might indicate your data is of poor quality.'
                self.log.warning(w)
                col_stats['empty_cells_score_warning'] = w
            else:
                col_stats['empty_cells_score_warning'] = None

            if col_stats['data_type_distribution_score'] < 3:
                #self.log.infoChart(stats[col_name]['data_type_dist'], type='list', uid='Dubious Data Type Distribution for column "{}"'.format(col_name))
                percentage_of_data_not_of_principal_type = col_stats['data_type_distribution_score'] * 100
                principal_data_type = col_stats['data_type']
                w = f'{percentage_of_data_not_of_principal_type}% of your data is not of type {principal_data_type}, which was detected to be the data type for column {col_name}, this might indicate your data is of poor quality.'
                self.log.warning(w)
                col_stats['data_type_distribution_score_warning'] = w
            else:
                col_stats['data_type_distribution_score_warning'] = None

            if 'z_test_based_outlier_score' in col_stats and col_stats['z_test_based_outlier_score'] < 4:
                percentage_of_outliers = col_stats['z_test_based_outlier_score']*100
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data is more than 3 standard deviations away from the mean, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['z_test_based_outlier_score_warning'] = w
            else:
                col_stats['z_test_based_outlier_score_warning'] = None

            if 'lof_based_outlier_score' in col_stats and col_stats['lof_based_outlier_score'] < 4:
                percentage_of_outliers = col_stats['percentage_of_log_based_outliers']
                w = f"""Column {col_name} has a very high amount of outliers, {percentage_of_outliers}% of your data doesn't fit closely in any cluster using the KNN algorithm (20n) to cluster your data, this means there might
                be too much randomness in this column for us to make an accurate prediction based on it."""
                self.log.warning(w)
                col_stats['lof_based_outlier_score_warning'] = w
            else:
                col_stats['lof_based_outlier_score_warning'] = None

            if col_stats['value_distribution_score'] < 9:
                max_probability_key = col_stats['max_probability_key']
                w = f"""Column {col_name} is very biased towards the value {max_probability_key}, please make sure that the data in this column is correct !"""
                self.log.warning(w)
                col_stats['value_distribution_score_warning'] = w
            else:
                col_stats['value_distribution_score_warning'] = None

            if col_stats['similarity_score'] < 6:
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

            # We might want to inform the user about a few stats regarding his column regardless of the score, this is done bellow
            self.log.info('Data distribution for column "{}"'.format(col_name))
            self.log.infoChart(stats[col_name]['data_subtype_dist'], type='list', uid='Data Type Distribution for column "{}"'.format(col_name))


    def run(self, input_data, modify_light_metadata, hmd=None, print_logs=True):
        """
        # Runs the stats generation phase
        # This shouldn't alter the columns themselves, but rather provide the `stats` metadata object and update the types for each column
        # A lot of information about the data distribution and quality will  also be logged to the server in this phase
        """

        no_processes = multiprocessing.cpu_count() - 2
        if no_processes < 1:
            no_processes = 1
        pool = multiprocessing.Pool(processes=no_processes)

        if print_logs == False:
            self.log = logging.getLogger('null-logger')
            self.log.propagate = False

        # we dont need to generate statistic over all of the data, so we subsample, based on our accepted margin of error
        population_size = len(input_data.data_frame)

        if population_size < 50:
            sample_size = population_size
        else:
            sample_size = int(calculate_sample_size(population_size=population_size, margin_error=self.transaction.lmd['sample_margin_of_error'], confidence_level=self.transaction.lmd['sample_confidence_level']))
            #if sample_size > 3000 and sample_size > population_size/8:
            #    sample_size = min(round(population_size/8),3000)

        # get the indexes of randomly selected rows given the population size
        input_data_sample_indexes = random.sample(range(population_size), sample_size)
        self.log.info('population_size={population_size},  sample_size={sample_size}  {percent:.2f}%'.format(population_size=population_size, sample_size=sample_size, percent=(sample_size/population_size)*100))

        all_sampled_data = input_data.data_frame.iloc[input_data_sample_indexes]

        stats = {}
        col_data_dict = {}

        for col_name in all_sampled_data.columns.values:
            col_data = all_sampled_data[col_name].dropna()
            full_col_data = all_sampled_data[col_name]

            data_type, curr_data_subtype, data_type_dist, data_subtype_dist, additional_info, column_status = self._get_column_data_type(col_data, input_data.data_frame, col_name)


            if column_status == 'Column empty':
                if modify_light_metadata:
                    self.transaction.lmd['malformed_columns']['names'].append(col_name)
                    self.transaction.lmd['malformed_columns']['indices'].append(i)
                continue

            new_col_data = []
            if curr_data_subtype == DATA_SUBTYPES.TIMESTAMP: #data_type == DATA_TYPES.DATE:
                for element in col_data:
                    if str(element) in [str(''), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null']:
                        new_col_data.append(None)
                    else:
                        try:
                            new_col_data.append(int(parse_datetime(element).timestamp()))
                        except:
                            self.log.warning(f'Could not convert string from col "{col_name}" to date and it was expected, instead got: {element}')
                            new_col_data.append(None)
                col_data = new_col_data
            if data_type == DATA_TYPES.NUMERIC or curr_data_subtype == DATA_SUBTYPES.TIMESTAMP:
                histogram, _ = StatsGenerator.get_histogram(col_data, data_type=data_type, data_subtype=curr_data_subtype)
                x = histogram['x']
                y = histogram['y']

                col_data = StatsGenerator.clean_int_and_date_data(col_data)
                # This means the column is all nulls, which we don't handle at the moment
                if len(col_data) < 1:
                    return None

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
            elif data_type == DATA_TYPES.CATEGORICAL or curr_data_subtype == DATA_SUBTYPES.DATE:
                histogram, _ = StatsGenerator.get_histogram(input_data.data_frame[col_name], data_type=data_type, data_subtype=curr_data_subtype)

                col_stats = {
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
                    "histogram": histogram,
                    "percentage_buckets": histogram['x']
                }

            elif curr_data_subtype == DATA_SUBTYPES.IMAGE:
                histogram, percentage_buckets = StatsGenerator.get_histogram(col_data, data_subtype=curr_data_subtype)

                col_stats = {
                    'data_type': data_type,
                    'data_subtype': curr_data_subtype,
                    'percentage_buckets': percentage_buckets,
                    'histogram': histogram
                }

            # @TODO This is probably wrong, look into it a bit later
            else:
                # see if its a sentence or a word
                histogram, _ = StatsGenerator.get_histogram(col_data, data_type=data_type, data_subtype=curr_data_subtype)
                dictionary = list(histogram.keys())

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
                    is_full_text = True if curr_data_subtype == DATA_SUBTYPES.TEXT else False
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

            empty_count = len(full_col_data) - len(col_data)

            stats[col_name]['empty_cells'] = empty_count
            stats[col_name]['empty_percentage'] = empty_count * 100 / len(full_col_data)
            if 'separator' in additional_info:
                stats[col_name]['separator'] = additional_info['separator']
            col_data_dict[col_name] = col_data

        for col_name in all_sampled_data.columns:
            if col_name in self.transaction.lmd['malformed_columns']['names']:
                continue

            # Use the multiprocessing pool for computing scores which take a very long time to compute
            # For now there's only one and computing it takes way too long, so this is not enabled
            scores = []

            '''
            scores.append(pool.apply_async(compute_clf_based_correlation_score, args=(stats, all_sampled_data, col_name)))
            '''
            for score_promise in scores:
                # Wait for function on process to finish running
                score = score_promise.get()
                stats[col_name].update(score)

            for score_func in [compute_duplicates_score, compute_empty_cells_score, compute_data_type_dist_score, compute_z_score, compute_lof_score, compute_similariy_score, compute_value_distribution_score]:
                start_time = time.time()
                if 'compute_z_score' in str(score_func) or 'compute_lof_score' in str(score_func):
                    stats[col_name].update(score_func(stats, col_data_dict, col_name))
                else:
                    stats[col_name].update(score_func(stats, all_sampled_data, col_name))

                fun_name = str(score_func)
                run_duration = round(time.time() - start_time, 2)
                #print(f'Running scoring function "{run_duration}" took {run_duration} seconds !')

            stats[col_name].update(compute_consistency_score(stats, col_name))
            stats[col_name].update(compute_redundancy_score(stats, col_name))
            stats[col_name].update(compute_variability_score(stats, col_name))

            stats[col_name].update(compute_data_quality_score(stats, col_name))

        total_rows = len(input_data.data_frame)

        if modify_light_metadata:
            self.transaction.lmd['column_stats'] = stats

            if 'sample_margin_of_error' in self.transaction.lmd and self.transaction.lmd['sample_margin_of_error'] is not None:
                self.transaction.lmd['data_preparation']['accepted_margin_of_error'] = self.transaction.lmd['sample_margin_of_error']
            else:
                self.transaction.lmd['data_preparation']['accepted_margin_of_error'] = CONFIG.DEFAULT_MARGIN_OF_ERROR

            self.transaction.lmd['data_preparation']['total_row_count'] = total_rows
            self.transaction.lmd['data_preparation']['used_row_count'] = sample_size
            self.transaction.lmd['data_preparation']['test_row_count'] = len(input_data.test_indexes[KEY_NO_GROUP_BY])
            self.transaction.lmd['data_preparation']['train_row_count'] = len(input_data.train_indexes[KEY_NO_GROUP_BY])
            self.transaction.lmd['data_preparation']['validation_row_count'] = len(input_data.validation_indexes[KEY_NO_GROUP_BY])

        pool.close()
        pool.join()

        self._log_interesting_stats(stats)

        return stats
