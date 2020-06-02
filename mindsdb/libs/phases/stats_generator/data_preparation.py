import random

import numpy as np
from dateutil.parser import parse as parse_datetime

from mindsdb.external_libs.stats import calculate_sample_size
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.text_helpers import clean_float


def sample_data(df, sample_margin_of_error, sample_confidence_level, log):
    population_size = len(df)

    sample_size = int(calculate_sample_size(population_size, sample_margin_of_error, sample_confidence_level)) if population_size > 50 else population_size
    sample_size_pct = sample_size*100/population_size

    # get the indexes of randomly selected rows given the population size
    input_data_sample_indexes = random.sample(range(population_size), sample_size)

    log.info(f'Analyzing a sample of {sample_size} from a total population of {population_size}, this is equivalent to {sample_size_pct}% of your data.')

    return df.iloc[input_data_sample_indexes]


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
