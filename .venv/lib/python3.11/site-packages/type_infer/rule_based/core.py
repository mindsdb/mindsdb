import re
import imghdr
import sndhdr
import multiprocessing as mp
from typing import List, Union
from collections import Counter

import numpy as np
import pandas as pd

from type_infer.dtype import dtype
from type_infer.base import BaseEngine, TypeInformation
from type_infer.helpers import log, seed, sample_data, get_nr_procs, is_nan_numeric, cast_string_to_python_type
from type_infer.rule_based.helpers import get_language_dist, analyze_sentences, get_identifier_description_mp


class RuleBasedEngine(BaseEngine):
    def __init__(self, config=None):
        """
        :param config: a dictionary containing the configuration for the engine
                pct_invalid : float
                    The percentage, i.e. a float between 0.0 and 100.0, of invalid values that are
                    accepted before failing the type inference for a column.
                seed : int, optional
                    Seed for the random number generator, by default 420
                mp_cutoff : int, optional
                    How many elements in the dataframe before switching to parallel processing, by
                    default 1e4.
        """
        super().__init__(stable=True)
        self.config = config if config else {'pct_invalid': 2, 'seed': 420, 'mp_cutoff': 1e4}

    def infer(self, data: pd.DataFrame) -> TypeInformation:
        seed(self.config['seed'])
        type_information = TypeInformation()
        sample_df = sample_data(data)
        sample_size = len(sample_df)
        population_size = len(data)
        log.info(f'Analyzing a sample of {sample_size}')
        log.info(
            f'from a total population of {population_size}, this is equivalent to {round(sample_size * 100 / population_size, 1)}% of your data.')  # noqa

        nr_procs = get_nr_procs(df=sample_df)
        pool_size = min(nr_procs, len(sample_df.columns.values))
        if data.size > self.config['mp_cutoff'] and pool_size > 1:
            log.info(f'Using {pool_size} processes to deduct types.')
            pool = mp.Pool(processes=pool_size)
            # column-wise parallelization  # TODO: evaluate switching to row-wise split instead
            answer_arr = pool.starmap(self.get_column_data_type, [
                (sample_df[x].dropna(), data[x], x, self.config['pct_invalid']) for x in sample_df.columns.values
            ])
            pool.close()
            pool.join()
        else:
            answer_arr = []
            for x in sample_df.columns:
                answer_arr.append(self.get_column_data_type(sample_df[x].dropna(), data, x, self.config['pct_invalid']))

        for i, col_name in enumerate(sample_df.columns):
            (data_dtype, data_dtype_dist, additional_info, warn, info) = answer_arr[i]

            for msg in warn:
                log.warning(msg)
            for msg in info:
                log.info(msg)

            if data_dtype is None:
                data_dtype = dtype.invalid

            type_information.dtypes[col_name] = data_dtype
            type_information.additional_info[col_name] = {
                'dtype_dist': data_dtype_dist
            }

        if data.size > self.config['mp_cutoff'] and pool_size > 1:
            pool = mp.Pool(processes=pool_size)
            answer_arr = pool.map(get_identifier_description_mp, [
                (data[x], x, type_information.dtypes[x])
                for x in sample_df.columns
            ])
            pool.close()
            pool.join()
        else:
            answer_arr = []
            for x in sample_df.columns:
                answer = get_identifier_description_mp([data[x], x, type_information.dtypes[x]])
                answer_arr.append(answer)

        for i, col_name in enumerate(sample_df.columns):
            # work with the full data
            if answer_arr[i] is not None:
                log.warning(f'Column {col_name} is an identifier of type "{answer_arr[i]}"')
                type_information.identifiers[col_name] = answer_arr[i]

        # @TODO Column removal logic was here, if the column was an identifier, move it elsewhere
        return type_information

    # @TODO: hardcode for distance, time, subunits of currency (e.g. cents) and other common units
    # @TODO: Add tests with plenty of examples

    def get_quantity_col_info(self, col_data: pd.Series) -> str:
        assert isinstance(col_data, pd.Series)
        char_const = None
        nr_map = set()
        for val in col_data:
            val = str(val)
            char_part = re.sub("[0-9.,]", '', val)
            numeric_bit = re.sub("[^0-9.,]", '', val).replace(',', '.')

            if len(char_part) == 0:
                char_part = None

            if len(re.sub("[^0-9]", '', numeric_bit)) == 0 or numeric_bit.count('.') > 1:
                numeric_bit = None
            else:
                numeric_bit = float(numeric_bit)

            if numeric_bit is None:
                return False, None
            else:
                nr_map.add(numeric_bit)

            if char_const is None:
                char_const = char_part

            if char_part is None or char_part == '-' or char_part != char_const:
                return False, None

        if len(nr_map) > 20 and len(nr_map) > len(col_data) / 200:
            return True, {char_const: {
                'multiplier': 1
            }}
        else:
            return False, None

    def get_binary_type(self, element: object) -> str:
        try:
            is_img = imghdr.what(element)
            if is_img is not None:
                return dtype.image

            # @TODO: currently we don differentiate between audio and video
            is_audio = sndhdr.what(element)
            # apparently `sndhdr` is really bad..
            for audio_ext in ['.wav', '.mp3']:
                if element.endswith(audio_ext):
                    is_audio = True
            if is_audio is not None:
                return dtype.audio
        except Exception:
            # Not a file or file doesn't exist
            return None

    def get_numeric_type(self, element: object) -> str:
        """ Returns the subtype inferred from a number string, or False if its not a number"""
        string_as_nr = cast_string_to_python_type(str(element))

        try:
            if string_as_nr == int(string_as_nr):
                string_as_nr = int(string_as_nr)
        except Exception:
            pass

        if isinstance(string_as_nr, float):
            return dtype.float
        elif isinstance(string_as_nr, int):
            return dtype.integer
        else:
            try:
                if is_nan_numeric(element):
                    return dtype.integer
                else:
                    return None
            except Exception:
                return None

    def type_check_sequence(self, element: object) -> str:
        dtype_guess = None

        if isinstance(element, List):
            all_nr = all([self.get_numeric_type(ele) for ele in element])
            if all_nr:
                dtype_guess = dtype.num_array
            else:
                dtype_guess = dtype.cat_array
        else:
            for sep_char in [',', '\t', '|', ' ']:  # @TODO: potential bottleneck, cutoff after a while
                all_nr = True
                if '[' in element:
                    ele_arr = element.rstrip(']').lstrip('[').split(sep_char)
                else:
                    ele_arr = element.rstrip(')').lstrip('(').split(sep_char)

                for ele in ele_arr:
                    if not self.get_numeric_type(ele):
                        all_nr = False
                        break

                if len(ele_arr) > 1 and all_nr:
                    dtype_guess = dtype.num_array

        return dtype_guess

    @staticmethod
    def type_check_date(element: object) -> str:
        """
        Check if element corresponds to a date-like object.
        """
        # check if element represents a date (no hour/minute/seconds)
        is_date = False
        # check if element represents a datetime (has hour/minute/seconds)
        is_datetime = False
        # check if it makes sense to convert element to unix time-stamp by
        # evaluating if, when converted, the element represents a number that
        # is compatible with a Unix timestamp (number of seconds since 1970-01-01T:00:00:00)
        # note that we also check the number is not larger than the "epochalypse time",
        # which is when the unix timestamp becomes larger than 2^32 - 1 seconds. We do
        # this because timestamps outside this range are likely to be unreliable and hence
        # rather treated as every-day numbers.
        min_dt = pd.to_datetime('1970-01-01 00:00:00', utc=True)
        max_dt = pd.to_datetime('2038-01-19 03:14:08', utc=True)
        valid_units = {'ns': 'unix', 'us': 'unix', 'ms': 'unix', 's': 'unix',
                       'D': 'julian'}
        for unit, origin in valid_units.items():
            try:
                as_dt = pd.to_datetime(element, unit=unit, origin=origin,
                                       errors='raise')
                if min_dt < as_dt < max_dt:
                    is_datetime = True
                    break
            except Exception:
                pass
        # check if element represents a date-like object.
        # here we don't check for a validity range like with unix-timestamps
        # because dates as string usually represent something more general than
        # just the number of seconds since an epoch.
        try:
            as_dt = pd.to_datetime(element, errors='raise')
            is_datetime = True
        except Exception:
            pass
        # finally, if element is represents a datetime object, check if only
        # date part is contained (no time information)
        if is_datetime:
            # round element day (drop hour/minute/second)
            dt_d = as_dt.to_period('D').to_timestamp()
            # if rounded datetime equals the datetime itself, it means there was not
            # hour/minute/second information to begin with. Mind the 'localize' to
            # avoid time-zone BS to kick in.
            is_date = dt_d == as_dt.tz_localize(None)
        if is_date:
            return dtype.date
        if is_datetime:
            return dtype.datetime

        return None

    def count_data_types_in_column(self, data):
        dtype_counts = Counter()

        type_checkers = [self.get_numeric_type,
                         self.type_check_sequence,
                         self.get_binary_type,
                         self.type_check_date]

        for element in data:
            for type_checker in type_checkers:
                try:
                    dtype_guess = type_checker(element)
                except Exception:
                    dtype_guess = None
                if dtype_guess is not None:
                    dtype_counts[dtype_guess] += 1
                    break
            else:
                dtype_counts[dtype.invalid] += 1

        return dtype_counts

    def get_column_data_type(self,
                             data: Union[pd.Series, np.ndarray, list],
                             full_data: pd.DataFrame,
                             col_name: str,
                             pct_invalid: float
                             ):
        """
        Provided the column data, define its data type and data subtype.

        :param data: an iterable containing a sample of the data frame
        :param full_data: an iterable containing the whole column of a data frame

        :return: type and type distribution, we can later use type_distribution to determine data quality
        NOTE: type distribution is the count that this column has for belonging cells to each DATA_TYPE
        """
        log.info(f'Infering type for: {col_name}')
        additional_info = {'other_potential_dtypes': []}

        warn = []
        info = []
        if len(data) == 0:
            warn.append(f'Column {col_name} has no data in it. ')
            warn.append(f'Please remove {col_name} from the training file or fill in some of the values !')
            return None, None, additional_info, warn, info

        dtype_counts = self.count_data_types_in_column(data)

        known_dtype_dist = {k: v for k, v in dtype_counts.items()}
        if dtype.float in known_dtype_dist and dtype.integer in known_dtype_dist:
            known_dtype_dist[dtype.float] += known_dtype_dist[dtype.integer]
            del known_dtype_dist[dtype.integer]

        if dtype.datetime in known_dtype_dist and dtype.date in known_dtype_dist:
            known_dtype_dist[dtype.datetime] += known_dtype_dist[dtype.date]
            del known_dtype_dist[dtype.date]

        max_known_dtype, max_known_dtype_count = max(
            known_dtype_dist.items(),
            key=lambda kv: kv[1]
        )

        actual_pct_invalid = 100 * (len(data) - max_known_dtype_count) / len(data)
        if max_known_dtype is None or max_known_dtype == dtype.invalid:
            curr_dtype = None
        elif actual_pct_invalid > self.config['pct_invalid']:
            if max_known_dtype in (dtype.integer, dtype.float) and actual_pct_invalid <= 5 * self.config['pct_invalid']:
                curr_dtype = max_known_dtype
            else:
                curr_dtype = None
        else:
            curr_dtype = max_known_dtype

        nr_vals = len(data)
        nr_distinct_vals = len(set([str(x) for x in data]))

        # Is it a quantity?
        if curr_dtype not in (dtype.datetime, dtype.date):
            is_quantity, quantitiy_info = self.get_quantity_col_info(data)
            if is_quantity:
                additional_info['quantitiy_info'] = quantitiy_info
                curr_dtype = dtype.quantity
                known_dtype_dist = {
                    dtype.quantity: nr_vals
                }

        # Check for Tags subtype
        if curr_dtype not in (dtype.quantity, dtype.num_array):
            lengths = []
            unique_tokens = set()

            can_be_tags = False
            if all(isinstance(x, str) for x in data):
                can_be_tags = True

            mean_lenghts = np.mean(lengths) if len(lengths) > 0 else 0

            # If more than 30% of the samples contain more than 1 category and there's more than 6 and less than 30 of them and they are shared between the various cells # noqa
            if (can_be_tags and mean_lenghts > 1.3 and
                    6 <= len(unique_tokens) <= 30 and
                    len(unique_tokens) / mean_lenghts < (len(data) / 4)):
                curr_dtype = dtype.tags

        # Categorical based on unique values
        if curr_dtype not in (dtype.date, dtype.datetime, dtype.tags, dtype.cat_array):
            if curr_dtype in (dtype.integer, dtype.float):
                is_categorical = nr_distinct_vals < 10
            else:
                is_categorical = nr_distinct_vals < min(max((nr_vals / 100), 10), 3000)

            if is_categorical:
                if curr_dtype is not None:
                    additional_info['other_potential_dtypes'].append(curr_dtype)
                curr_dtype = dtype.categorical

        # If curr_data_type is still None, then it's text or category
        if curr_dtype is None:
            log.info(f'Doing text detection for column: {col_name}')
            lang_dist = get_language_dist(data)  # TODO: bottleneck

            # Normalize lang probabilities
            for lang in lang_dist:
                lang_dist[lang] /= len(data)

            # If most cells are unknown language then it's categorical
            if lang_dist['Unknown'] > 0.5:
                curr_dtype = dtype.categorical
            else:
                nr_words, word_dist, nr_words_dist = analyze_sentences(data)  # TODO: maybe pass entire corpus at once

                if 1 in nr_words_dist and nr_words_dist[1] == nr_words:
                    curr_dtype = dtype.categorical
                else:
                    if len(word_dist) > 500 and nr_words / len(data) > 5:
                        curr_dtype = dtype.rich_text
                    else:
                        curr_dtype = dtype.short_text

                    return curr_dtype, {curr_dtype: len(data)}, additional_info, warn, info

        if curr_dtype in [dtype.categorical, dtype.rich_text, dtype.short_text, dtype.cat_array]:
            known_dtype_dist = {curr_dtype: len(data)}

        if nr_distinct_vals < 3 and curr_dtype == dtype.categorical:
            curr_dtype = dtype.binary
            known_dtype_dist[dtype.binary] = known_dtype_dist[dtype.categorical]
            del known_dtype_dist[dtype.categorical]

        log.info(f'Column {col_name} has data type {curr_dtype}')
        return curr_dtype, known_dtype_dist, additional_info, warn, info

