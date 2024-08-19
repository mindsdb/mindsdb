import os
import psutil
import random
import logging
import colorlog
import multiprocessing as mp
from typing import Iterable

import numpy as np
import pandas as pd
from scipy.stats import norm


def initialize_log():
    pid = os.getpid()
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter())

    logging.basicConfig(handlers=[handler])
    log = logging.getLogger(f'type_infer-{pid}')
    log_level = os.environ.get('TYPE_INFER_LOG', 'DEBUG')
    log.setLevel(log_level)
    return log


log = initialize_log()


def get_nr_procs(df=None):
    if 'MINDSDB_N_WORKERS' in os.environ:
        try:
            n = int(os.environ['MINDSDB_N_WORKERS'])
        except ValueError:
            n = 1
        return n
    elif os.name == 'nt':
        return 1
    else:
        available_mem = psutil.virtual_memory().available
        if df is not None:
            max_per_proc_usage = df.size
        else:
            max_per_proc_usage = 0.2 * pow(10, 9)  # multiplier * 1GB

        proc_count = int(min(mp.cpu_count() - 1, available_mem // max_per_proc_usage))

        return max(proc_count, 1)


def seed(seed_nr: int) -> None:
    np.random.seed(seed_nr)
    random.seed(seed_nr)


def is_nan_numeric(value: object) -> bool:
    """
    Determines if **value** might be `nan` or `inf` or some other numeric value (i.e. which can be cast as `float`) that is not actually a number.
    """  # noqa
    if isinstance(value, np.ndarray) or (type(value) != str and isinstance(value, Iterable)):
        return False

    try:
        value = str(value)
        value = float(value)
    except Exception:
        return False

    try:
        if isinstance(value, float):
            a = int(value) # noqa
        isnan = False
    except Exception:
        isnan = True
    return isnan


def cast_string_to_python_type(string):
    """ Returns None, an integer, float or a string from a string"""
    if string is None or string == '':
        return None

    if string.isnumeric():
        # Did you know you can write fractions in unicode, and they are numeric but can't be cast to integers !?
        try:
            return int(string)
        except Exception:
            return None

    try:
        return clean_float(string)
    except Exception:
        return string


def clean_float(val):
    if isinstance(val, (int, float)):
        return float(val)

    if isinstance(val, float):
        return val

    val = str(val).strip(' ')
    val = val.replace(',', '.')
    val = val.rstrip('"').lstrip('"')

    if val in ('', '.', 'None', 'nan'):
        return None

    try:
        return float(val)
    except Exception:
        return None


def sample_data(df: pd.DataFrame) -> pd.DataFrame:
    population_size = len(df)
    if population_size <= 50:
        sample_size = population_size
    else:
        sample_size = int(round(_calculate_sample_size(population_size)))

    population_size = len(df)
    input_data_sample_indexes = random.sample(range(population_size), sample_size)
    return df.iloc[input_data_sample_indexes]


def _calculate_sample_size(
    population_size,
    margin_error=.01,
    confidence_level=.995,
    sigma=1 / 2
):
    """
    Calculate the minimal sample size to use to achieve a certain
    margin of error and confidence level for a sample estimate
    of the population mean.
    Inputs
    -------
    population_size: integer
        Total size of the population that the sample is to be drawn from.
    margin_error: number
        Maximum expected difference between the true population parameter,
        such as the mean, and the sample estimate.
    confidence_level: number in the interval (0, 1)
        If we were to draw a large number of equal-size samples
        from the population, the true population parameter
        should lie within this percentage
        of the intervals (sample_parameter - e, sample_parameter + e)
        where e is the margin_error.
    sigma: number
        The standard deviation of the population.  For the case
        of estimating a parameter in the interval [0, 1], sigma=1/2
        should be sufficient.
    """
    alpha = 1 - confidence_level
    # dictionary of confidence levels and corresponding z-scores
    # computed via norm.ppf(1 - (alpha/2)), where norm is
    # a normal distribution object in scipy.stats.
    # Here, ppf is the percentile point function.
    zdict = {
        .90: 1.645,
        .91: 1.695,
        .99: 2.576,
        .97: 2.17,
        .94: 1.881,
        .93: 1.812,
        .95: 1.96,
        .98: 2.326,
        .96: 2.054,
        .92: 1.751
    }
    if confidence_level in zdict:
        z = zdict[confidence_level]
    else:
        # Inf fix
        if alpha == 0.0:
            alpha += 0.001
        z = norm.ppf(1 - (alpha / 2))
    N = population_size
    M = margin_error
    numerator = z**2 * sigma**2 * (N / (N - 1))
    denom = M**2 + ((z**2 * sigma**2) / (N - 1))
    return numerator / denom
