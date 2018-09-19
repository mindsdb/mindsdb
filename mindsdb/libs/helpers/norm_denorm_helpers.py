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

def norm(value, cell_stats):


    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.NUMERIC:

        if (str(value) in [str(''), str(' '), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA'] or (
                value == None or value == '' or value == '\n' or value == '\r')):
            return [0, 1]

        if cell_stats['max'] - cell_stats['min'] != 0:

            normalizedValue = (value - cell_stats['min']) / \
                              (cell_stats['max'] - cell_stats['min'])

        elif cell_stats['max'] != 0:
            normalizedValue = value / cell_stats['max']
        else:
            normalizedValue = value

        if normalizedValue > 10:
            raise ValueError('Something is wrong with normalized value')

        return [normalizedValue, 0]

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.DATE:
        #[ timestamp, year, month, day, minute, second, is null]
        if (str(value) in [str(''), str(' '), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA'] or (
                value == None or value == '' or value == '\n' or value == '\r')):
            ret = [0]*7
            ret[-1] = 1
            return ret

        try:
            timestamp = int(parseDate(value).timestamp())
        except:
            ret = [0] * 7
            ret[-1] = 1
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

        norm_vals.append(0)

        return norm_vals

    if cell_stats[KEYS.DATA_TYPE] == DATA_TYPES.TEXT:
        # is it a word
        if cell_stats['dictionaryAvailable']:
            # all the words in the dictionary +2 (one for rare words and one for null)
            vector_length = len(cell_stats['dictionary']) + TEXT_ENCODING_EXTRA_LENGTH
            arr = [0] * vector_length
            if value in [None, '']:
                # return NULL value, which is an empy hot vector array with the last item in list with value 1
                arr[vector_length - 1] = 1  # set null as 1
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
            return [0, 1]

        # is it a full text
        if cell_stats['dictionaryAvailable']:
            # all the words in the dictionary +2 (one for rare words and one for null)
            vector_length = len(cell_stats['dictionary']) + FULL_TEXT_ENCODING_EXTRA_LENGTH
            arr = [0] * vector_length
            if value in [None, '']:
                # return NULL value, which is an empty hot vector array with the last item in list with value 1
                return [[vector_length - 1]]

            # else return a list of one hot vectors
            values = splitRecursive(value, WORD_SEPARATORS)
            array_of_arrays = []
            first_word = vector_length - 4

            array_of_arrays += [first_word]
            for word in values:
                # else return one hot vector
                # if word is a strange word it will not be in the dictionary
                try:
                    index = cell_stats['dictionary'].index(word)
                except:
                    index = vector_length - 2
                array_of_arrays += [index]

            last_word = vector_length - 3

            array_of_arrays += [last_word]
            # return [array_of_arrays]
            # TODO: ask about this
            return array_of_arrays

        else:

            return []

def denorm(value, cell_stats, return_nones = True, return_dates_as_time_stamps = False):

    # TODO: Get a format for dates
    if abs(value[-1]) >= 1 and cell_stats[KEYS.DATA_TYPE] != DATA_TYPES.TEXT:
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
            if value[-1] == 1:
                return ''

            if value[-2] == 1:
                return '*'
            max = -100
            index = None
            for j, v in enumerate(value):
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