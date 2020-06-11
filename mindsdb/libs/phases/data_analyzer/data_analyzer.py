import logging
from collections import Counter, defaultdict

import pandas as pd
import numpy as np
from scipy.stats import entropy
from dateutil.parser import parse as parse_datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import MiniBatchKMeans
import imagehash
from PIL import Image

from mindsdb.libs.helpers.general_helpers import get_value_bucket
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive, clean_float
from mindsdb.libs.phases.data_analyzer.scores import (
    compute_duplicates_score,
    compute_empty_cells_score,
    compute_data_type_dist_score,
    compute_similariy_score,
    compute_value_distribution_score,
    compute_z_score,
    compute_lof_score,
    compute_consistency_score,
    compute_redundancy_score,
    compute_variability_score,
    compute_data_quality_score
)
from mindsdb.libs.helpers.stats_helpers import sample_data


def clean_int_and_date_data(col_data, log):
    cleaned_data = []

    for ele in col_data:
        if str(ele) not in ['', str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA', 'null'] and (not ele or not str(ele).isspace()):
            try:
                cleaned_data.append(clean_float(ele))
            except Exception as e1:
                try:
                    cleaned_data.append(parse_datetime(str(ele)).timestamp())
                except Exception as e2:
                    log.warning(f'Failed to parser numerical value with error chain:\n {e1} -> {e2}\n')
                    cleaned_data.append(0)

    return cleaned_data


def get_hist(data):
    counts = Counter(data)
    return {
        'x': list(counts.keys()),
        'y': list(counts.values())
    }


def get_text_histogram(data):
    """ If text, returns an array of all the words that appear in the dataset
        and the number of times each word appears in the dataset """
    words = []
    for cell in data:
        words += splitRecursive(cell, WORD_SEPARATORS)

    hist = get_hist(words)
    return hist


def get_numeric_histogram(data, data_subtype):
    Y, X = np.histogram(data, bins=min(50, len(set(data))),
                        range=(min(data), max(data)), density=False)
    if data_subtype == DATA_SUBTYPES.INT:
        Y, X = np.histogram(data, bins=[int(round(x)) for x in X], density=False)

    X = X[:-1].tolist()
    Y = Y.tolist()

    return {
       'x': X,
       'y': Y
    }


def get_image_histogram(data):
    image_hashes = []
    for img_path in data:
        img_hash = imagehash.phash(Image.open(img_path))
        seq_hash = []
        for hash_row in img_hash.hash:
            seq_hash.extend(hash_row)

        image_hashes.append(np.array(seq_hash))

    kmeans = MiniBatchKMeans(n_clusters=20,
                             batch_size=round(len(image_hashes) / 4))

    kmeans.fit(image_hashes)

    x = []
    y = [0] * len(kmeans.cluster_centers_)

    for cluster in kmeans.cluster_centers_:
        similarities = cosine_similarity(image_hashes, kmeans.cluster_centers_)

        similarities = list(map(lambda x: sum(x), similarities))

        index_of_most_similar = similarities.index(max(similarities))
        x.append(data.iloc[index_of_most_similar])

    indices = kmeans.predict(image_hashes)
    for index in indices:
        y[index] += 1

    return {
               'x': x,
               'y': y
           }, list(kmeans.cluster_centers_)


def get_histogram(data, data_type, data_subtype):
    """ Returns a histogram for the data and [optionaly] the percentage buckets"""
    if data_subtype == DATA_SUBTYPES.TEXT:
        return get_text_histogram(data), None
    elif data_subtype == DATA_SUBTYPES.ARRAY:
        return get_hist(data), None
    elif data_type == DATA_TYPES.NUMERIC or data_subtype == DATA_SUBTYPES.TIMESTAMP:
        hist = get_numeric_histogram(data, data_subtype)
        return hist, hist['x']
    elif data_type == DATA_TYPES.CATEGORICAL or data_subtype == DATA_SUBTYPES.DATE:
        hist = get_hist(data)
        hist = {str(k): v for k, v in hist.items()}
        return hist, hist['x']
    elif data_subtype == DATA_SUBTYPES.IMAGE:
        return get_image_histogram(data)
    else:
        return None, None


def get_column_empty_values_report(data):
    len_wo_nulls = len(data.dropna())
    len_w_nulls = len(data)
    nr_missing_values = len_w_nulls - len_wo_nulls

    ed = {
        'empty_cells': len_w_nulls - len_wo_nulls,
        'empty_percentage': 100 * round(nr_missing_values/ len_w_nulls, 3),
        'is_empty': len_wo_nulls == 0
        ,'description': 'TBD'
    }
    if nr_missing_values > 0:
        ed['warning'] = f'Your column has {nr_missing_values} values missing'

    return ed


def get_uniq_values_report(data):
    len_unique = len(set(data))
    ud = {
        'unique_values': len_unique,
        'unique_percentage': 100 * round(len_unique / len(data), 8)
        ,'description': 'TBD'
    }
    if len_unique == 1:
        ud['warning'] = 'This column contains no information because it has a single possible value.'

    return ud


def compute_entropy_biased_buckets(hist_y, hist_x):
    S, biased_buckets = None, None
    nr_values = sum(hist_y)
    S = entropy([x / nr_values for x in hist_y], base=max(2, len(hist_y)))
    if S < 0.25:
        pick_nr = -max(1, int(len(hist_y) / 10))
        biased_buckets = [hist_x[i] for i in np.array(hist_y).argsort()[pick_nr:]]
    return S, biased_buckets


def compute_outlier_buckets(outlier_values,
                            hist_x,
                            hist_y,
                            percentage_buckets,
                            col_stats):
    outlier_buckets = []
    # map each bucket to list of outliers in it
    bucket_outliers = defaultdict(list)
    for value in outlier_values:
        vb_index = get_value_bucket(value,
                                    percentage_buckets,
                                    col_stats)
        vb = percentage_buckets[vb_index]
        bucket_outliers[vb].append(value)

    # Filter out buckets without outliers,
    # then sort by number of outliers in ascending order
    buckets_with_outliers = sorted(filter(
        lambda kv: len(kv[1]) > 0, bucket_outliers.items()
    ), key=lambda kv: len(kv[1]))

    for i, (bucket, outlier_values) in enumerate(buckets_with_outliers):
        bucket_index = hist_x.index(bucket)

        bucket_values_num = hist_y[bucket_index]
        bucket_outliers_num = len(outlier_values)

        # Is the bucket in the 95th percentile by number of outliers?
        percentile_outlier = ((i + 1) / len(buckets_with_outliers)) >= 0.95

        # Are half of values in the bucket outliers?
        predominantly_outlier = False
        if bucket_values_num:
           predominantly_outlier = (bucket_outliers_num / bucket_values_num) > 0.5

        if predominantly_outlier or percentile_outlier:
            outlier_buckets.append(bucket)
    return outlier_buckets


class DataAnalyzer(BaseModule):
    """
    The data analyzer phase is responsible for generating the insights we need about the data in order to vectorize it.
    Additionally, also provides the user with some extra meaningful information about his data.
    """

    # @TODO get rid of scores and stats entirely
    def compute_scores(self, col_name, sample_df, full_df, stats):
        for score_func in [compute_duplicates_score,
                           compute_empty_cells_score,
                           compute_data_type_dist_score,
                           compute_similariy_score,
                           compute_value_distribution_score,
                           ]:
            score_out = score_func(stats, sample_df, col_name)
            stats[col_name].update(score_out)

        for score_func in [compute_z_score,
                           compute_lof_score]:
            score_out = score_func(stats, full_df, col_name)
            stats[col_name].update(score_out)

        for score_func in [compute_consistency_score,
                           compute_redundancy_score,
                           compute_variability_score,
                           compute_data_quality_score]:
            score_out = score_func(stats, col_name)
            stats[col_name].update(score_out)

    def run(self, input_data):
        stats = dict(self.transaction.lmd['column_stats'])
        stats_v2 = dict(self.transaction.lmd['stats_v2'])
        col_data_dict = {}

        sample_df = sample_data(input_data.data_frame,
                                self.transaction.lmd['sample_margin_of_error'],
                                self.transaction.lmd['sample_confidence_level'],
                                self.log)

        for col_name in self.transaction.lmd['empty_columns']:
            stats_v2[col_name] = {}
            stats_v2[col_name]['empty'] = {'is_empty': True}
            self.log.warning(f'Column {col_name} is empty.')

        for col_name in sample_df.columns.values:
            self.log.info(f'Analyzing column: {col_name} !')
            data_type = stats_v2[col_name]['typing']['data_type']
            data_subtype = stats_v2[col_name]['typing']['data_subtype']

            col_data = sample_df[col_name].dropna()
            if data_type == DATA_TYPES.NUMERIC or data_subtype == DATA_SUBTYPES.TIMESTAMP:
                col_data = clean_int_and_date_data(col_data, self.log)
            col_data_dict[col_name] = col_data

            stats_v2[col_name]['empty'] = get_column_empty_values_report(input_data.data_frame[col_name])

            stats[col_name]['empty_cells'] = stats_v2[col_name]['empty']['empty_cells']
            stats[col_name]['empty_percentage'] = stats_v2[col_name]['empty']['empty_percentage']

            hist_data = col_data
            if data_type == DATA_TYPES.CATEGORICAL:
                hist_data = input_data.data_frame[col_name]
                stats_v2[col_name]['unique'] = get_uniq_values_report(input_data.data_frame[col_name])

            histogram, percentage_buckets = get_histogram(hist_data,
                                                          data_type=data_type,
                                                          data_subtype=data_subtype)
            stats_v2[col_name]['histogram'] = histogram
            stats_v2[col_name]['percentage_buckets'] = percentage_buckets
            stats[col_name]['histogram'] = histogram
            stats[col_name]['percentage_buckets'] = percentage_buckets
            if histogram:
                S, biased_buckets = compute_entropy_biased_buckets(histogram['y'], histogram['x'])
                stats_v2[col_name]['bias'] = {
                    'entropy': S,
                    'description': 'TBD'
                }
                if biased_buckets:
                    stats_v2[col_name]['bias']['biased_buckets'] = biased_buckets
                if S < 0.8:
                    if data_type in (DATA_TYPES.CATEGORICAL):
                        warning_str =  "You may to check if some categories occur too often to too little in this columns."
                    else:
                        warning_str = "You may want to check if you see something suspicious on the right-hand-side graph."
                    stats_v2[col_name]['bias']['warning'] = warning_str + " This doesn't necessarily mean there's an issue with your data, it just indicates a higher than usual probability there might be some issue."

            self.compute_scores(col_name, sample_df, col_data_dict, stats)

            if 'lof_outliers' in stats[col_name]:
                stats_v2[col_name]['outliers'] = {
                    'outlier_values': stats[col_name]['lof_outliers'],
                    'outlier_score': stats[col_name]['lof_based_outlier_score'],
                    'outlier_buckets': compute_outlier_buckets(outlier_values=stats[col_name]['lof_outliers'],
                                                               hist_x=histogram['x'],
                                                               hist_y=histogram['y'],
                                                               percentage_buckets=percentage_buckets,
                                                               col_stats=stats[col_name]),
                    'description': 'TBD'
                }

            stats_v2[col_name]['nr_warnings'] = 0
            for x in stats_v2[col_name].values():
                if isinstance(x, dict) and 'warning' in x:
                    self.log.warning(x['warning'])
                stats_v2[col_name]['nr_warnings'] += 1
            self.log.info(f'Finished analyzing column: {col_name} !\n')

        self.transaction.lmd['column_stats'].update(stats)
        self.transaction.lmd['stats_v2'].update(stats_v2)

        self.transaction.lmd['data_preparation']['accepted_margin_of_error'] = self.transaction.lmd['sample_margin_of_error']

        self.transaction.lmd['data_preparation']['total_row_count'] = len(input_data.data_frame)
        self.transaction.lmd['data_preparation']['used_row_count'] = len(sample_df)
