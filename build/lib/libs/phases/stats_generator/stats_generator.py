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
import traceback
import sys

import numpy as np
import pandas as pd
import scipy.stats as st
from dateutil.parser import parse as parseDate

import config as CONFIG

from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from libs.helpers.text_helpers import splitRecursive
from external_libs.stats import sampleSize

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

    def saveStats(self, model_name, stats):

        """ Saves the stats on a mongo db collection """

        # if testing offline don't save the stats into a collection but into a file

        model_stats = self.session.mongo.mindsdb.model_stats
        model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                          {'$set': {
                              "model_name": model_name,
                              'submodel_name': None,
                              KEY_COLUMNS: self.transaction.input_metadata[KEY_COLUMNS],
                              "stats": stats
                          }}, upsert=True)

        self.transaction.model_stats = stats
        return

    def run(self):
        model_name = self.transaction.model_metadata[KEY_MODEL_NAME]
        model_stats = self.session.mongo.mindsdb.model_stats

        header = self.transaction.input_metadata[KEY_COLUMNS]
        origData = {}
        for column in header:
            origData[column] = []

        empty_count = {}
        column_count = {}

        population_size = len(self.transaction.input_data_array)
        sample_size = int(sampleSize(population_size=population_size, margin_error=CONFIG.DEFAULT_MARGIN_OF_ERROR, confidence_level=CONFIG.DEFAULT_CONFIDENCE_LEVEL))
        input_data_sample_indexes = random.sample(range(population_size), sample_size)
        self.logging.info('population_size={population_size},  sample_size={sample_size}  {percent:.2f}%'.format(population_size=population_size, sample_size=sample_size, percent=(sample_size/population_size)*100))
        meta = self.transaction.model_metadata
        meta['status_percentage'] = 0
        model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                               {'$set': {
                                   'submodel_name': None,
                                    KEY_COLUMNS: self.transaction.input_metadata[KEY_COLUMNS],
                                   "model_metadata": meta
                               }}, upsert=True)
        for sample_i in input_data_sample_indexes:
            row = self.transaction.input_data_array[sample_i]
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
                    col_data[i] = int(parseDate(element).timestamp())
            if data_type == DATA_TYPES.NUMERIC or data_type == DATA_TYPES.DATE:
                newData = []

                for value in col_data:
                    if value != '' and value != '\r' and value != '\n':
                        newData.append(value)
                col_data = newData
                col_data = [float(i) for i in newData if str(i) not in ['', None, False, np.nan, 'NaN', 'nan']]
                data = pd.Series(col_data)
                # best_fit_name, bestFitParamms, x, y = self.getBestFitDistribution(
                #     data)
                # best_dist = getattr(st, best_fit_name)
                y, x = np.histogram(col_data, 50, density=False)
                x = (x + np.roll(x, -1))[:-1] / 2.0
                x = x.tolist()
                y = y.tolist()
                if len(col_data) > 0:
                    max_value = max(col_data)
                    min_value = min(col_data)
                    mean = np.mean(col_data)
                    median = np.median(col_data)
                    var = np.var(col_data)
                    skew = st.skew(col_data)
                    kurtosis = st.kurtosis(col_data)
                else:
                    max_value = 0
                    min_value = 0
                    mean = 0
                    median = 0
                    var = 0
                    skew = 0
                    kurtosis = 0

                # distribution_params = self.getParamsAsDictionary(bestFitParamms)

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
                    }
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

            meta = self.transaction.model_metadata
            meta['status_percentage'] =  (i +1)/ len(origData) * 100
            if self.transaction.input_test_data_array:
                train_rows = len(self.transaction.input_data_array)
                test_rows = len(self.transaction.input_test_data_array)
                total_rows = train_rows + test_rows
            else:
                total_rows = len(self.transaction.input_data_array)
                test_rows = int(total_rows * CONFIG.TEST_TRAIN_RATIO)
                train_rows = total_rows - test_rows

            model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                                   {'$set': {
                                       "model_metadata":meta,
                                       "global_stats": {
                                           "total_rows": total_rows,
                                           "test_rows": test_rows,
                                           "train_rows": train_rows
                                       }
                                   }}, upsert=True)
            self.saveStats(model_name, stats)
        return stats


def test():
    from libs.test.test_controller import TestController
    TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.diamonds) AS diamonds PREDICT price', PHASE_DATA_STATS)



# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
