from typing import Dict, Optional
import dateinfer
import datetime

import numpy as np
import pandas as pd
from scipy.stats import entropy
from dateutil.parser import parse as parse_dt

from type_infer.dtype import dtype
from type_infer.api import infer_types

from dataprep_ml.cleaners import cleaner
from dataprep_ml.cleaners import _clean_float
from dataprep_ml.helpers import get_ts_groups
from dataprep_ml.helpers import filter_nan_and_none

from dataprep_ml.helpers import log
from dataprep_ml.helpers import seed
from dataprep_ml.base import StatisticalAnalysis, DataAnalysis


def analyze_dataset(df: pd.DataFrame, target: Optional[str] = None, args: Optional[dict] = None) -> DataAnalysis:
    """
    Use this to understand and visualize the data.

    :param df: Raw data in dataframe format.
    :param args: additional arguments to pass into either the type inference or statistical analysis phases.
    :returns: An object containing insights about the data (specifically the type information and statistical analysis).
    """  # noqa
    if target is None:
        target = df.columns[0]
    if args is None:
        args = {'target': target}
    else:
        args['target'] = target

    type_information = infer_types(df)
    stats = statistical_analysis(df, type_information.dtypes, args, type_information.identifiers)
    return DataAnalysis(type_information=type_information, statistical_analysis=stats)


def statistical_analysis(data: pd.DataFrame,
                         dtypes: Dict[str, str],
                         args,  # TODO: refactor this so that input is not ProblemDefinition but normal Dict
                         identifiers: Dict[str, object] = {},
                         exceptions: list = ['__mdb_original_index'],   # TODO: add this as default in lw, rename  __lw_
                         seed_nr: int = 420) -> StatisticalAnalysis:
    seed(seed_nr)
    log.info('Starting statistical analysis')

    tss = args.get('timeseries_settings', {})
    if tss.get('is_timeseries', False):
        oby_col = tss['order_by']
        try:
            order_format = infer_date_format(data[oby_col])
        except Exception:
            order_format = None

    nr_columns = len(data.columns)
    df = cleaner(data, dtypes, args.get('pct_invalid', 0),
                 args['target'], tss, args.get('anomaly_detection', False),
                 mode='train', identifiers=identifiers)
    columns = [col for col in df.columns if col not in exceptions]

    missing = {}
    distinct = {}
    for col in columns:
        missing[col] = {
            'missing': len([x for x in df[col] if x is None]) / len(df[col]) if len(df[col]) else 0,
            'description': 'Proportion of missing values for the column. Columns with high % of missing values may not be as useful for modelling purposes.'  # noqa
        }
        distinct[col] = len(set([str(x) for x in df[col]])) / len(df[col]) if len(df[col]) else 0

    nr_rows = len(df)
    target = args['target']
    positive_domain = False
    # get train std, used in analysis
    if dtypes[target] in [dtype.float, dtype.integer, dtype.num_tsarray, dtype.quantity]:
        df_std = df[target].astype(float).std()
        if min(df[target]) >= 0:
            positive_domain = True
    elif dtypes[target] in [dtype.num_array]:
        try:
            all_vals = []
            for x in df[target]:
                all_vals += x
            if min(all_vals) >= 0:
                positive_domain = True
            df_std = pd.Series(all_vals).astype(float).std()
        except Exception as e:
            log.warning(e)
            df_std = 1.0
    else:
        df_std = 1.0

    histograms = {}
    buckets = {}
    # Get histograms for each column
    for col in columns:
        histograms[col] = None
        buckets[col] = None
        if dtypes[col] in (dtype.categorical, dtype.binary, dtype.tags):
            hist = dict(df[col].value_counts())
            histograms[col] = {
                'x': list([str(x) for x in hist.keys()]),
                'y': list(hist.values())
            }
            buckets[col] = histograms[col]['x']
        elif dtypes[col] in (dtype.integer, dtype.float, dtype.num_tsarray, dtype.quantity):
            histograms[col] = get_numeric_histogram(filter_nan_and_none(df[col]), dtypes[col], 50)
            buckets[col] = histograms[col]['x']
        elif dtypes[col] in (dtype.date, dtype.datetime):
            histograms[col] = get_datetime_histogram(filter_nan_and_none(df[col]), 50)
        # @TODO: case for num_ and cat_ arrays
        else:
            histograms[col] = {'x': ['Unknown'], 'y': [len(df[col])]}
            buckets[col] = []

    # get observed classes, used in analysis
    target_class_distribution = None
    target_weights = None
    if dtypes[target] in (dtype.categorical, dtype.binary, dtype.cat_tsarray):
        target_class_distribution = dict(df[target].value_counts().apply(lambda x: x / len(df[target])))
        target_weights = {}
        for k in target_class_distribution:
            target_weights[k] = 1 / target_class_distribution[k]
        train_observed_classes = list(target_class_distribution.keys())
    elif dtypes[target] == dtype.tags:
        train_observed_classes = None  # @TODO: pending call to tags logic -> get all possible tags
    else:
        train_observed_classes = None

    bias = {}
    for col in columns:
        S, biased_buckets = compute_entropy_biased_buckets(histograms[col])
        bias[col] = {
            'entropy': S,
            'description': """"Potential bias" is flagged when data does not distribute normally or uniformly, likely over-representing or under-representing some values. This may be normal, hence bias is only "potential".""", # noqa
            'biased_buckets': biased_buckets
        }

    avg_words_per_sentence = {}
    for col in columns:
        if dtypes[col] in (dtype.rich_text, dtype.short_text):
            words_per_sentence = []
            for item in df[col]:
                if item is not None:
                    words_per_sentence.append(len(item.split(' ')))
            avg_words_per_sentence[col] = int(np.mean(words_per_sentence))
        else:
            avg_words_per_sentence[col] = None

    if tss.get('is_timeseries', False):
        groups = get_ts_groups(data, tss)
        ts_stats = {
            'groups': groups,
            'order_format': order_format
        }
    else:
        ts_stats = {}

    log.info('Finished statistical analysis')
    return StatisticalAnalysis(
        nr_rows=nr_rows,
        nr_columns=nr_columns,
        df_target_stddev=df_std,
        train_observed_classes=train_observed_classes,
        target_class_distribution=target_class_distribution,
        target_weights=target_weights,
        positive_domain=positive_domain,
        histograms=histograms,
        buckets=buckets,
        missing=missing,
        distinct=distinct,
        bias=bias,
        avg_words_per_sentence=avg_words_per_sentence,
        ts_stats=ts_stats
    )


def get_datetime_histogram(data: pd.Series, bins: int) -> Dict[str, list]:
    """Generates the histogram for date and datetime types
    """
    if isinstance(data.iloc[0], float) or isinstance(data.iloc[0], int):
        data = _clean_float(data)
    elif isinstance(data.iloc[0], pd.Timestamp):
        # TODO: this should probably be deleted
        data = data.apply(lambda x: x.timestamp())
    else:
        # TODO: slow, optimize
        data = [_clean_float(parse_dt(str(x)).timestamp()) for x in data]

    Y, X = np.histogram(data, bins=min(bins, len(set(data))),
                        range=(min(data), max(data)), density=False)

    X = X[:-1].tolist()
    Y = Y.tolist()

    X = [str(datetime.datetime.fromtimestamp(x)) for x in X]
    return {
        'x': X,
        'y': Y
    }


def get_numeric_histogram(data: pd.Series, data_dtype: dtype, bins: int) -> Dict[str, list]:
    """Generate the histogram for integer and float typed data
    """
    # Handle arrays that are actual arrays and not things that become arrays later
    if isinstance(data.iloc[0], list):
        data = data.applymap(lambda x: x[0]).values.reshape(-1, 1)

    data = _clean_float(data)

    Y, X = np.histogram(data, bins=min(bins, len(set(data))),
                        range=(min(data), max(data)), density=False)
    if data_dtype == dtype.integer:
        Y, X = np.histogram(data, bins=[int(round(x)) for x in X], density=False)

    X = X[:-1].tolist()
    Y = Y.tolist()

    return {
        'x': X,
        'y': Y
    }


def compute_entropy_biased_buckets(histogram):
    S, biased_buckets = None, None
    if histogram is not None or len(histogram['x']) == 0:
        hist_x = histogram['x']
        hist_y = histogram['y']
        nr_values = sum(hist_y)
        S = entropy([x / nr_values for x in hist_y], base=max(2, len(hist_y)))
        if S < 0.25:
            pick_nr = -max(1, int(len(hist_y) / 10))
            biased_buckets = [hist_x[i] for i in np.array(hist_y).argsort()[pick_nr:]]
    return S, biased_buckets


def infer_date_format(values: pd.DataFrame, n_samples=50) -> str:
    """ infer date format based on a small sample """
    return dateinfer.infer(
        values.sample(n=min(n_samples, len(values))).tolist()
    )
