import imghdr
import sndhdr
from collections import Counter, defaultdict
from dateutil.parser import parse as parse_datetime

from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES, DATA_TYPES_SUBTYPES
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import (word_tokenize,
                                               cast_string_to_python_type,
                                               is_foreign_key)
from mindsdb.libs.helpers.stats_helpers import sample_data


def get_file_subtype_if_exists(path):
    try:
        is_img = imghdr.what(path)
        if is_img is not None:
            return DATA_SUBTYPES.IMAGE

        # @TODO: CURRENTLY DOESN'T DIFFERENTIATE BETWEEN AUDIO AND VIDEO
        is_audio = sndhdr.what(path)
        if is_audio is not None:
            return DATA_SUBTYPES.AUDIO
    except Exception:
        # Not a file or file doesn't exist
        pass


def get_number_subtype(string):
    """ Returns the subtype inferred from a number string, or False if its not a number"""
    string = str(string)
    python_type = type(cast_string_to_python_type(string))
    if python_type is float:
        return DATA_SUBTYPES.FLOAT
    elif python_type is int:
        return DATA_SUBTYPES.INT
    else:
        return None


def get_text_category_subtype(data):
    """Takes in column data of text values and defines its categorical subtype.
    If categorical, returns DATA_SUBTYPES.MULTIPLE or DATA_SUBTYPES.SINGLE.
    If not categorical, returns None"""

    key_count = Counter()
    max_number_of_words = 0

    for cell in data:
        key_count[cell] += 1

        words = word_tokenize(cell)

        if max_number_of_words < words:
            max_number_of_words += words

    # If all sentences are less than or equal and 3 words,
    # assume it's a category rather than a sentence
    if max_number_of_words <= 3:
        if len(key_count.keys()) < 3:
            return DATA_SUBTYPES.SINGLE
        else:
            return DATA_SUBTYPES.MULTIPLE


class TypeDeductor(BaseModule):
    """
    The type deduction phase is responsible for inferring data types
    from cleaned data
    """

    def count_data_types_in_column(self, data):
        type_counts = Counter()
        subtype_counts = Counter()
        additional_info = {}

        def type_check_numeric(element):
            type_guess, subtype_guess = None, None
            subtype = get_number_subtype(element)
            if subtype is not None:
                type_guess = DATA_TYPES.NUMERIC
                subtype_guess = subtype
            return type_guess, subtype_guess

        def type_check_date(element):
            type_guess, subtype_guess = None, None
            try:
                dt = parse_datetime(element)

                # Not accurate 100% for a single datetime str,
                # but should work in aggregate
                if dt.hour == 0 and dt.minute == 0 and \
                    dt.second == 0 and len(element) <= 16:
                    subtype_guess = DATA_SUBTYPES.DATE
                else:
                    subtype_guess = DATA_SUBTYPES.TIMESTAMP
                type_guess = DATA_TYPES.DATE
            except ValueError:
                pass
            return type_guess, subtype_guess

        def type_check_sequence(element):
            type_guess, subtype_guess = None, None
            for char in [',', '\t', '|', ' ']:
                all_nr = True
                eles = element.rstrip(']').lstrip('[').split(char)
                for ele in eles:
                    if not get_number_subtype(ele):
                        all_nr = False
                        break

                if all_nr:
                    additional_info['separator'] = char
                    type_guess = DATA_TYPES.SEQUENTIAL
                    subtype_guess = DATA_SUBTYPES.ARRAY

            return type_guess, subtype_guess

        def type_check_file(element):
            type_guess, subtype_guess = None, None
            subtype = get_file_subtype_if_exists(element)
            if subtype:
                type_guess = DATA_TYPES.FILE_PATH
                subtype_guess = subtype
            return type_guess, subtype_guess

        type_checkers = [type_check_numeric,
                         type_check_date,
                         type_check_sequence,
                         type_check_file]
        for element in data:
            data_type_guess, subtype_guess = None, None
            for type_checker in type_checkers:
                data_type_guess, subtype_guess = type_checker(element)
                if data_type_guess:
                    break

            if not data_type_guess:
                data_type_guess = 'Unknown'
                subtype_guess = 'Unknown'

            type_counts[data_type_guess] += 1
            subtype_counts[subtype_guess] += 1

        return type_counts, subtype_counts, additional_info

    def get_column_data_type(self, data, full_data, col_name):
        """
        Provided the column data, define its data type and data subtype.

        :param data: an iterable containing a sample of the data frame
        :param full_data: an iterable containing the whole column of a data frame

        :return: type and type distribution, we can later use type_distribution to determine data quality
        NOTE: type distribution is the count that this column has for belonging cells to each DATA_TYPE
        """
        type_dist, subtype_dist = {}, {}
        additional_info = {'other_potential_subtypes': [], 'other_potential_types': []}

        if col_name in self.transaction.lmd['data_subtypes']:
            curr_data_type = self.transaction.lmd['data_types'][col_name]
            curr_data_subtype = self.transaction.lmd['data_subtypes'][col_name]
            type_dist[curr_data_type] = len(data)
            subtype_dist[curr_data_subtype] = len(data)
            self.log.info(f'Manually setting the types for column {col_name} to {curr_data_type}->{curr_data_subtype}')
            return curr_data_type, curr_data_subtype, type_dist, subtype_dist, additional_info

        if len(data) == 0:
            self.log.warning(f'Column {col_name} has no data in it. '
                             f'Please remove {col_name} from the training file or fill in some of the values !')
            return None, None, None, None, additional_info

        if col_name in self.transaction.lmd['force_categorical_encoding']:
            curr_data_type = DATA_TYPES.CATEGORICAL
            curr_data_subtype = DATA_SUBTYPES.MULTIPLE
            type_dist[curr_data_type] = len(data)
            subtype_dist[curr_data_subtype] = len(data)
            return curr_data_type, curr_data_subtype, type_dist, subtype_dist, additional_info

        type_dist, subtype_dist, new_additional_info = self.count_data_types_in_column(data)
        if new_additional_info:
            additional_info.update(new_additional_info)

        # @TODO consider removing or flagging rows where data type is unknown in the future, might just be corrupt data...
        known_type_dist = {k: v for k, v in type_dist.items() if k != 'Unknown'}
        max_known_dtype, max_known_dtype_count = None, None
        if known_type_dist:
            max_known_dtype, max_known_dtype_count = max(known_type_dist.items(), key=lambda pair: pair[0])
        if max_known_dtype and max_known_dtype_count > type_dist['Unknown']:
            # Data is mostly not unknown, go with type counting results
            curr_data_type = max_known_dtype

            possible_subtype_counts = [(k, v) for k, v in subtype_dist.items()
                                       if k in DATA_TYPES_SUBTYPES.subtypes[curr_data_type]]
            curr_data_subtype, _ = max(possible_subtype_counts,
                                       key=lambda pair: pair[0])

        else:
            # Data contains a lot of Unknown, assume text or categorical
            categorical_subtype = get_text_category_subtype(data)
            if categorical_subtype:
                curr_data_type, curr_data_subtype = DATA_TYPES.CATEGORICAL, categorical_subtype
                type_dist[DATA_TYPES.CATEGORICAL] = type_dist['Unknown']
                subtype_dist[categorical_subtype] = subtype_dist['Unknown']
            else:
                curr_data_type, curr_data_subtype = DATA_TYPES.SEQUENTIAL, DATA_SUBTYPES.TEXT
                type_dist[DATA_TYPES.SEQUENTIAL] = type_dist['Unknown']
                subtype_dist[DATA_SUBTYPES.TEXT] = subtype_dist['Unknown']

            del type_dist['Unknown']
            del subtype_dist['Unknown']

        if curr_data_type not in [DATA_TYPES.CATEGORICAL, DATA_TYPES.DATE]:
            all_values = full_data
            all_distinct_vals = set(all_values)

            # The numbers here are picked randomly,
            # the gist of it is that if values repeat themselves a lot
            # we should consider the column to be categorical
            nr_vals = len(all_values)
            nr_distinct_vals = len(all_distinct_vals)

            if ((curr_data_subtype == DATA_SUBTYPES.TEXT and self.transaction.lmd['handle_text_as_categorical'])
                or ((nr_distinct_vals < nr_vals / 20) and (curr_data_type not in [DATA_TYPES.NUMERIC, DATA_TYPES.DATE] or nr_distinct_vals < 20))):

                additional_info['other_potential_types'].append(curr_data_type)
                additional_info['other_potential_subtypes'].append(curr_data_subtype)
                curr_data_type = DATA_TYPES.CATEGORICAL
                if len(all_distinct_vals) < 3:
                    curr_data_subtype = DATA_SUBTYPES.SINGLE
                else:
                    curr_data_subtype = DATA_SUBTYPES.MULTIPLE
                type_dist = {
                    curr_data_type: len(data)
                }
                subtype_dist = {
                    curr_data_subtype: len(data)
                }

        return curr_data_type, curr_data_subtype, type_dist, subtype_dist, additional_info

    def run(self, input_data):
        stats = defaultdict(dict)
        stats_v2 = defaultdict(dict)

        # Really bad that these parameters are implicitly passed through lmd
        # Perhaps sampling can be moved somewhere upwards,
        # so that it can be reused by all downstream phases?
        sample_df = sample_data(input_data.data_frame,
                                self.transaction.lmd['sample_margin_of_error'],
                                self.transaction.lmd['sample_confidence_level'],
                                self.log)

        for col_name in sample_df.columns.values:
            col_data = sample_df[col_name].dropna()

            (data_type, data_subtype, data_type_dist,
             data_subtype_dist, additional_info) = self.get_column_data_type(col_data,
                                                                             input_data.data_frame[col_name],
                                                                             col_name)

            type_data = {
                'data_type': data_type,
                'data_subtype': data_subtype,
                'data_type_dist': data_type_dist,
                'data_subtype_dist': data_subtype_dist,
            }
            stats[col_name] = type_data
            stats[col_name].update(additional_info)
            stats_v2[col_name]['typing'] = type_data
            stats_v2[col_name]['additional_info'] = additional_info

            stats_v2[col_name]['is_foreign_key'] = is_foreign_key(col_data,
                                                                  col_name,
                                                                  data_subtype,
                                                                  additional_info['other_potential_subtypes'])
            stats[col_name]['is_foreign_key'] = stats_v2[col_name]['is_foreign_key']
            if stats_v2[col_name]['is_foreign_key'] and self.transaction.lmd['handle_foreign_keys']:
                self.transaction.lmd['columns_to_ignore'].append(col_name)

            if data_subtype_dist:
                self.log.info(f'Data distribution for column "{col_name}" '
                              f'of type "{data_type}" '
                              f'and subtype "{data_subtype}"')
                try:
                    self.log.infoChart(data_subtype_dist,
                                       type='list',
                                       uid=f'Data Type Distribution for column "{col_name}"')
                except Exception:
                    # Functionality is specific to mindsdb logger
                    pass

        if not self.transaction.lmd.get('column_stats'):
            self.transaction.lmd['column_stats'] = {}
        if not self.transaction.lmd.get('stats_v2'):
            self.transaction.lmd['stats_v2'] = {}

        self.transaction.lmd['column_stats'].update(stats)
        self.transaction.lmd['stats_v2'].update(stats_v2)


