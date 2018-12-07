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

from mindsdb.libs.constants.mindsdb import *
from dateutil.parser import parse as parseDate
from mindsdb.libs.helpers.text_helpers import splitRecursive
import numpy as np
import datetime

OFFSET = 0.00000001

def norm_buckets(value, cell_stats):
    '''

    This will return a list of len of cell_stats['percentage_buckets']+2
    Last two values in list are [greater_than_max_value, is_null]
    First value of list is [smaller than min]

    :param value: the value to encode
    :param cell_stats: the stats for the column
    :return: a list
    '''

    if cell_stats[KEYS.DATA_TYPE] in [DATA_TYPES.NUMERIC, DATA_TYPES.DATE]:
        ret = [0]*(len(cell_stats['percentage_buckets'])+2)
        # if noen return empty list
        if value == None:
            ret[-1] = 1
            return ret
        # else set the last item as 1


        # if it's greater than the maximum set the 'second last' to one
        if value > cell_stats['max']:
            ret[-2] = 1
            return ret

        for i,val in enumerate(cell_stats['percentage_buckets']):
            if (value <= val and i ==0):
                ret[0] = 1
                break
            elif (value <= val and value > cell_stats['percentage_buckets'][i-1]):
                ret[i] =1
                break

        return ret


    else:
        raise Exception('Should not try to get norm buckets for other data types than numeric and date')

def norm(value, cell_stats):


    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:

        if (str(value) in [str(''), str(' '), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA'] or (
                value == None or value == '' or value == '\n' or value == '\r')):
            return [0, 0]

        if cell_stats['max'] - cell_stats['min'] != 0:

            normalizedValue = (value - cell_stats['min']) / \
                              (cell_stats['max'] - cell_stats['min'])


        elif cell_stats['max'] != 0:
            normalizedValue = value / cell_stats['max']
        else:
            normalizedValue = value

        # if normalizedValue > 10:
        #     raise ValueError('Something is wrong with normalized value')

        sign = 1 if normalizedValue >= 0 else 0

        #normalizedValue = abs(normalizedValue) + OFFSET

        return [normalizedValue, 1.0]

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.DATE:
        #[ timestamp, year, month, day, minute, second, is null]
        if (str(value) in [str(''), str(' '), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA'] or (
                value == None or value == '' or value == '\n' or value == '\r')):
            ret = [0]*7
            ret[-1] = 0
            return ret

        try:
            timestamp = int(parseDate(value).timestamp())
        except:
            ret = [0] * 7
            ret[-1] = 0
            return ret
        date = datetime.datetime.fromtimestamp(timestamp)
        date_max = datetime.datetime.fromtimestamp(cell_stats['max'])
        date_min = datetime.datetime.fromtimestamp(cell_stats['min'])

        attrs = ['year', 'month', 'day', 'minute', 'second']
        maxes = {'day': 31, 'minute': 60, 'second': 60, 'month': 12}

        norm_vals = []

        if cell_stats['max'] - cell_stats['min'] != 0:
            norm_vals.append( (timestamp - cell_stats['min']) / (cell_stats['max'] - cell_stats['min']) )
        else:
            norm_vals.append( timestamp / cell_stats['max'] )

        for k_attr  in attrs:

            curr = getattr(date, k_attr)
            if k_attr in maxes:
                d_max = maxes[k_attr]
                d_min = 0
            else:
                d_max = getattr(date_max, k_attr)
                d_min = getattr(date_min, k_attr)

            if d_max - d_min !=0:
                norm_vals.append( (curr -d_min)/(d_max-d_min) )
            else:
                norm_vals.append((curr) / (d_max))

        norm_vals.append(1.0)

        return norm_vals

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.TEXT:
        # is it a word
        if cell_stats['dictionaryAvailable']:
            # all the words in the dictionary +2 (one for rare words and one for null)
            vector_length = len(cell_stats['dictionary']) + TEXT_ENCODING_EXTRA_LENGTH
            arr = [0] * vector_length
            arr[-1] = 1.0
            if value in [None, '']:
                # return NULL value, which is an empy hot vector array with the last item in list with value 1
                arr[vector_length - 1] = 0  # set null as 1
                return arr

            # else return one hot vector
            # if word is a strange word it will not be in the dictionary
            try:
                index = cell_stats['dictionary'].index(value)
            except:
                index = vector_length - 2

            arr[index] = 1
            return arr

        else:

            return []

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.FULL_TEXT:

        if (str(value) in [str(''), str(' '), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA'] or (
                value == None or value == '' or value == '\n' or value == '\r')):
            return [FULL_TEXT_NONE_VALUE]

        # is it a full text
        if cell_stats['dictionaryAvailable']:
            # all the words in the dictionary +2 (one for rare words and one for null)
            vector_length = len(cell_stats['dictionary']) + FULL_TEXT_ENCODING_EXTRA_LENGTH


            # else return a list of one hot vectors
            values = splitRecursive(value, WORD_SEPARATORS)
            array_of_arrays = []
            first_word = vector_length - 4

            array_of_arrays += [FULL_TEXT_IS_START]
            for word in values:
                # else return one hot vector
                # if word is a strange word it will not be in the dictionary
                try:
                    index = cell_stats['dictionary'].index(word)
                except:
                    index = FULL_TEXT_UN_FREQUENT
                array_of_arrays += [index]



            array_of_arrays += [FULL_TEXT_IS_END]
            # return [array_of_arrays]
            # TODO: ask about this
            return array_of_arrays

        else:

            return []

def denorm(value, cell_stats, return_nones = True, return_dates_as_time_stamps = False):

    # TODO: Get a format for dates
    if round(abs(value[-1])) <= 0 and cell_stats[KEYS.DATA_TYPE] != DATA_TYPES.TEXT:
        if return_nones:
            return None
        elif cell_stats[KEYS.DATA_TYPE] in [DATA_TYPES.NUMERIC, DATA_TYPES.DATE]:
            return 0
        else:
            return ''

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:

        value = value[0]

        if cell_stats['max'] - cell_stats['min'] != 0:
            denormalized = value * (cell_stats['max'] - cell_stats['min']) + cell_stats['min']
        else:
            denormalized = value * cell_stats['max']


        return denormalized

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.DATE:
        value = value[0]
        if cell_stats['max'] - cell_stats['min'] != 0:
            denormalized = value * (cell_stats['max'] - cell_stats['min']) + cell_stats['min']
        else:
            denormalized = value * cell_stats['max']
        # this should return a valid timestamp
        return denormalized

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.TEXT:
        if cell_stats['dictionaryAvailable']:
            not_null = True if value[-1] >= 0.5 else False
            other = True if value[-2] >= 0.5 else False

            if not_null is False:
                return ''

            if other is True:
                return '*'

            max = -100
            index = None

            for j, v in enumerate(value[:-2]):
                if v > max:
                    index = j
                    max = v


            return cell_stats['dictionary'][index]
        else:
            return ''

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.FULL_TEXT:
        # is it a full text
        text = []
        vector_length = len(cell_stats['dictionary']) + FULL_TEXT_ENCODING_EXTRA_LENGTH

        if not cell_stats['dictionaryAvailable']:
            return ''
        for word_array in value:
            # rare word.... how to handle this?
            # TODO: handl this better.
            if word_array == vector_length - 4 or word_array == vector_length - 3:
                # First or Last word
                continue
            else:
                if word_array == vector_length - 2:
                    # Rare
                    text.append('*')
                else:
                    if word_array == vector_length - 1:
                        # None
                        text.append('')
                    else:
                        # normal word
                        index = word_array
                        text.append(cell_stats['dictionary'][index])

        text = ' '.join(text)
        return text