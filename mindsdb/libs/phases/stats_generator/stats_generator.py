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

import random
import json
import time
import warnings
import logging
import traceback
import sys

import numpy as np
import pandas as pd
import scipy.stats as st
from dateutil.parser import parse as parseDate

import mindsdb.config as CONFIG

from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.helpers.text_helpers import splitRecursive
from mindsdb.external_libs.stats import sampleSize

class StatsGenerator(BaseModule):

    phase_name = PHASE_DATA_STATS

    def cast(self, string):
        """ Returns an integer, float or a string from a string"""
        try:
            if string is None:
                return None
            return int(string)
        except ValueError:
            try:
                return float(string)
            except ValueError:
                if string == '':
                    return None
                else:
                    return string

    def isNumber(self, string):
        """ Returns True if string is a number. """
        try:
            float(string)
            return True
        except ValueError:
            return False

    def isDate(self, string):
        """ Returns True if string is a valid date format """
        try:
            parseDate(string)
            return True
        except ValueError:
            return False

    def getColumnDataType(self, data):
        """ Returns the column datatype based on a random sample of 15 elements """
        currentGuess = DATA_TYPES.NUMERIC
        type_dist = {}

        for element in data:
            if self.isNumber(element):
                currentGuess = DATA_TYPES.NUMERIC
            elif self.isDate(element):
                currentGuess = DATA_TYPES.DATE
            else:
                currentGuess = DATA_TYPES.TEXT

            if currentGuess not in type_dist:
                type_dist[currentGuess] = 1
            else:
                type_dist[currentGuess] += 1

        curr_data_type = None
        max_data_type = 0

        for data_type in type_dist:
            if type_dist[data_type] > max_data_type:
                curr_data_type = data_type
                max_data_type = type_dist[data_type]

        if curr_data_type == DATA_TYPES.TEXT:
            return self.getTextType(data)

        return curr_data_type

    def getBestFitDistribution(self, data, bins=40):
        """Model data by finding best fit distribution to data"""
        # Get histogram of original data

        y, x = np.histogram(data, bins=bins, density=False)
        x = (x + np.roll(x, -1))[:-1] / 2.0
        # Distributions to check
        DISTRIBUTIONS = [
            st.bernoulli, st.beta,  st.cauchy, st.expon,  st.gamma, st.halfcauchy, st.lognorm,
            st.norm, st.uniform, st.poisson
        ]

        # Best holders
        best_distribution = st.norm
        best_params = (0.0, 1.0)
        best_sse = np.inf
        # Estimate distribution parameters from data
        for i, distribution in enumerate(DISTRIBUTIONS):
            try:
                # Ignore warnings from data that can't be fit
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore')
                    # fit dist to data
                    params = distribution.fit(data)
                    # Separate parts of parameters
                    arg = params[:-2]
                    loc = params[-2]
                    scale = params[-1]

                    # Calculate fitted PDF and error with fit in distribution
                    pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                    sse = np.sum(np.power(y - pdf, 2.0))
                    # identify if this distribution is better
                    if best_sse > sse > 0:
                        best_distribution = distribution
                        best_params = params
                        best_sse = sse

            except Exception:
                pass

        return (best_distribution.name, best_params, x.tolist(), y.tolist())

    def getTextType(self, data):

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
                cell_wseparator = cell_wseparator.replace(separator,sep_tag)

            words_split = cell_wseparator.split(sep_tag)
            words = len([ word for word in words_split if word not in ['', None] ])

            if max_number_of_words < words:
                max_number_of_words += words

        if max_number_of_words == 1:
            return DATA_TYPES.TEXT
        if max_number_of_words <= 3 and len(key_count) < total_length * 0.8:
            return DATA_TYPES.TEXT
        else:
            return DATA_TYPES.FULL_TEXT



    # def isFullText(self, data):
    #     """
    #     It determines if the column is full text right
    #     Right now we assume its full text if any cell contains any of the WORD_SEPARATORS
    #
    #     :param data: a list containing all the column
    #     :return: Boolean
    #     """
    #     for cell in data:
    #         try:
    #             if any(separator in cell for separator in WORD_SEPARATORS):
    #                 return True
    #         except:
    #             exc_type, exc_value, exc_traceback = sys.exc_info()
    #             error = traceback.format_exception(exc_type, exc_value,
    #                                       exc_traceback)
    #             return False
    #     return False





    def getWordsDictionary(self, data, full_text = False):
        """ Returns an array of all the words that appear in the dataset and the number of times each word appears in the dataset """

        splitter = lambda w, t: [wi.split(t) for wi in w] if type(w) == type([]) else splitter(w,t)

        if full_text:
            # get all words in every cell and then calculate histograms
            words = []
            for cell in data:
                words += splitRecursive(cell, WORD_SEPARATORS)

            hist = {i: words.count(i) for i in words}
            x = list(hist.keys())
            histogram = {
                'x': x,
                'y': list(hist.values())
            }
            return x, histogram


        else:
            hist = {i: data.count(i) for i in data}
            x = list(hist.keys())
            histogram = {
                'x': x,
                'y': list(hist.values())
            }
            return x, histogram

    def getParamsAsDictionary(self, params):
        """ Returns a dictionary with the params of the distribution """
        arg = params[:-2]
        loc = params[-2]
        scale = params[-1]
        ret = {
            'loc': loc,
            'scale': scale,
            'shape': arg
        }
        return ret



    def run(self):

        header = self.transaction.input_data.columns
        origData = {}

        for column in header:
            origData[column] = []

        empty_count = {}
        column_count = {}

        # we dont need to generate statistic over all of the data, so we subsample, based on our accepted margin of error
        population_size = len(self.transaction.input_data.data_array)
        sample_size = int(sampleSize(population_size=population_size, margin_error=CONFIG.DEFAULT_MARGIN_OF_ERROR, confidence_level=CONFIG.DEFAULT_CONFIDENCE_LEVEL))

        # get the indexes of randomly selected rows given the population size
        input_data_sample_indexes = random.sample(range(population_size), sample_size)
        self.logging.info('population_size={population_size},  sample_size={sample_size}  {percent:.2f}%'.format(population_size=population_size, sample_size=sample_size, percent=(sample_size/population_size)*100))

        for sample_i in input_data_sample_indexes:
            row = self.transaction.input_data.data_array[sample_i]
            for i, val in enumerate(row):
                column = header[i]
                value = self.cast(val)
                if not column in empty_count:
                    empty_count[column] = 0
                    column_count[column] = 0
                if value == None:
                    empty_count[column] += 1
                else:
                    origData[column].append(value)
                column_count[column] += 1
        stats = {}

        for i, col_name in enumerate(origData):
            col_data = origData[col_name] # all rows in just one column
            data_type = self.getColumnDataType(col_data)
            if data_type == DATA_TYPES.DATE:
                for i, element in enumerate(col_data):
                    if str(element) in [str(''), str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA']:
                        col_data[i] = None
                    else:
                        try:
                            col_data[i] = int(parseDate(element).timestamp())
                        except:
                            logging.warning('Could not convert string to date and it was expected, current value {value}'.format(value=element))
                            col_data[i] = None

            if data_type == DATA_TYPES.NUMERIC or data_type == DATA_TYPES.DATE:
                newData = []

                for value in col_data:
                    if value != '' and value != '\r' and value != '\n':
                        newData.append(value)


                col_data = [float(i) for i in newData if str(i) not in ['', str(None), str(False), str(np.nan), 'NaN', 'nan', 'NA']]

                y, x = np.histogram(col_data, 50, density=False)
                x = (x + np.roll(x, -1))[:-1] / 2.0
                x = x.tolist()
                y = y.tolist()

                xp = []

                if len(col_data) > 0:
                    max_value = max(col_data)
                    min_value = min(col_data)
                    mean = np.mean(col_data)
                    median = np.median(col_data)
                    var = np.var(col_data)
                    skew = st.skew(col_data)
                    kurtosis = st.kurtosis(col_data)

                    inc_rate = 0.04
                    initial_step_size = abs(max_value-min_value)/100

                    xp += [min_value]
                    i = min_value + initial_step_size

                    while i < max_value:

                        xp += [i]
                        i_inc = abs(i-min_value)*inc_rate
                        i = i + i_inc


                    # TODO: Solve inc_rate for N
                    #    min*inx_rate + (min+min*inc_rate)*inc_rate + (min+(min+min*inc_rate)*inc_rate)*inc_rate ....
                    #
                    #      x_0 = 0
                    #      x_i = (min+x_(i-1)) * inc_rate = min*inc_rate + x_(i-1)*inc_rate
                    #
                    #      sum of x_i_{i=1}^n (x_i) = max_value = inc_rate ( n * min + sum(x_(i-1)) )
                    #
                    #      mx_value/inc_rate = n*min + inc_rate ( n * min + sum(x_(i-2)) )
                    #
                    #     mx_value = n*min*in_rate + inc_rate^2*n*min + inc_rate^2*sum(x_(i-2))
                    #              = n*min(inc_rate+inc_rate^2) + inc_rate^2*sum(x_(i-2))
                    #              = n*min(inc_rate+inc_rate^2) + inc_rate^2*(inc_rate ( n * min + sum(x_(i-3)) ))
                    #              = n*min(sum_(i=1)^(i=n)(inc_rate^i))
                    #    =>  sum_(i=1)^(i=n)(inc_rate^i)) = max_value/(n*min(sum_(i=1)^(i=n))
                    #
                    # # i + i*x

                else:
                    max_value = 0
                    min_value = 0
                    mean = 0
                    median = 0
                    var = 0
                    skew = 0
                    kurtosis = 0
                    xp = []





                col_stats = {
                    "column": col_name,
                    KEYS.DATA_TYPE: data_type,
                    # "distribution": best_fit_name,
                    # "distributionParams": distribution_params,
                    "mean": mean,
                    "median": median,
                    "variance": var,
                    "skewness": skew,
                    "kurtosis": kurtosis,
                    "emptyColumns": empty_count[col_name],
                    "emptyPercentage": empty_count[col_name] / column_count[col_name] * 100,
                    "max": max_value,
                    "min": min_value,
                    "histogram": {
                        "x": x,
                        "y": y
                    },
                    "percentage_buckets": xp
                }
                stats[col_name] = col_stats
            # else if its text
            else:

                # see if its a sentence or a word
                is_full_text = True if data_type == DATA_TYPES.FULL_TEXT else False
                dictionary, histogram = self.getWordsDictionary(col_data, is_full_text)

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
                    if dictionary_lenght_percentage > 10 and len(col_data) > 50 and is_full_text==False:
                        dictionary = []
                        dictionary_available = False
                col_stats = {

                    "column": col_name,
                    KEYS.DATA_TYPE: DATA_TYPES.FULL_TEXT if is_full_text else data_type,
                    "dictionary": dictionary,
                    "dictionaryAvailable": dictionary_available,
                    "dictionaryLenghtPercentage": dictionary_lenght_percentage,
                    "emptyColumns": empty_count[col_name],
                    "emptyPercentage": empty_count[col_name] / column_count[col_name] * 100,
                    "histogram": histogram
                }
                stats[col_name] = col_stats



        total_rows = len(self.transaction.input_data.data_array)
        test_rows = len(self.transaction.input_data.test_indexes)
        validation_rows = len(self.transaction.input_data.validation_indexes)
        train_rows = len(self.transaction.input_data.train_indexes)

        self.transaction.persistent_model_metadata.column_stats = stats
        self.transaction.persistent_model_metadata.total_row_count = total_rows
        self.transaction.persistent_model_metadata.test_row_count = test_rows
        self.transaction.persistent_model_metadata.train_row_count = train_rows
        self.transaction.persistent_model_metadata.validation_row_count = validation_rows

        self.transaction.persistent_model_metadata.update()

        return stats



def test():
    from mindsdb.libs.controllers.mindsdb_controller import MindsDBController as MindsDB

    mdb = MindsDB()
    mdb.learn(from_query='select * from position_tgt', predict='position', model_name='mdsb_model', test_query=None, breakpoint = PHASE_DATA_STATS)

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

