from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.helpers.text_helpers import hashtext
from mindsdb.external_libs.stats import calculate_sample_size

from pandas.api.types import is_numeric_dtype
import random
import traceback
import pandas as pd
import numpy as np


class DataExtractor(BaseModule):
    def _get_data_frame_from_when_conditions(self):
        """
        :return:
        """
        when_conditions = self.transaction.hmd['model_when_conditions']

        when_conditions_list = []
        # here we want to make a list of the type  ( ValueForField1, ValueForField2,..., ValueForFieldN ), ...
        for when_condition in when_conditions:
            cond_list = [None] * len(self.transaction.lmd['columns'])  # empty list with blanks for values

            for condition_col in when_condition:
                col_index = self.transaction.lmd['columns'].index(condition_col)
                cond_list[col_index] = when_condition[condition_col]

            when_conditions_list.append(cond_list)

        result = pd.DataFrame(when_conditions_list, columns=self.transaction.lmd['columns'])

        return result


    def _apply_sort_conditions_to_df(self, df):
        """

        :param df:
        :return:
        """

        # apply order by (group_by, order_by)
        if self.transaction.lmd['model_is_time_series']:
            asc_values = [order_tuple[ORDER_BY_KEYS.ASCENDING_VALUE] for order_tuple in self.transaction.lmd['model_order_by']]
            sort_by = [order_tuple[ORDER_BY_KEYS.COLUMN] for order_tuple in self.transaction.lmd['model_order_by']]

            if self.transaction.lmd['model_group_by']:
                sort_by = self.transaction.lmd['model_group_by'] + sort_by
                asc_values = [True for i in self.transaction.lmd['model_group_by']] + asc_values
            df = df.sort_values(sort_by, ascending=asc_values)

        elif self.transaction.lmd['type'] == TRANSACTION_LEARN:
            # if its not a time series, randomize the input data and we are learning
            df = df.sample(frac=1, random_state=len(df))

        return df


    def _get_prepared_input_df(self):
        """

        :return:
        """
        df = None

        # if transaction metadata comes with some data as from_data create the data frame
        if 'from_data' in self.transaction.hmd and self.transaction.hmd['from_data'] is not None:
            # make sure we build a dataframe that has all the columns we need
            df = self.transaction.hmd['from_data']
            df = df.where((pd.notnull(df)), None)

        # if this is a predict statement, create use model_when_conditions to shape the dataframe
        if  self.transaction.lmd['type'] == TRANSACTION_PREDICT:
            if self.transaction.hmd['when_data'] is not None:
                df = self.transaction.hmd['when_data']
                df = df.where((pd.notnull(df)), None)

                for col in self.transaction.lmd['columns']:
                    if col not in df.columns:
                        df[col] = [None] * len(df)

            elif self.transaction.hmd['model_when_conditions'] is not None:

                # if no data frame yet, make one
                df = self._get_data_frame_from_when_conditions()


        # if by now there is no DF, throw an error
        if df is None:
            error = 'Could not create a data frame for transaction'
            self.log.error(error)
            raise ValueError(error)
            return None

        df = self._apply_sort_conditions_to_df(df)
        groups = df.columns.to_series().groupby(df.dtypes).groups

        if np.dtype('datetime64[ns]') in groups:
            for colname in groups[np.dtype('datetime64[ns]')]:
                df[colname] = df[colname].astype(str)

        return df


    def _validate_input_data_integrity(self):
        """
        :return:
        """
        if self.transaction.input_data.data_frame.shape[0] <= 0:
            error = 'Input Data has no rows, please verify from_data or when_conditions'
            self.log.error(error)
            raise ValueError(error)

        # make sure that the column we are trying to predict is on the input_data
        # else fail, because we cannot predict data we dont have

        #if self.transaction.lmd['model_is_time_series'] or self.transaction.lmd['type'] == TRANSACTION_LEARN:
        # ^ How did this even make sense before ? Why did it not crash tests ? Pressumably because the predict col was loaded into `input_data` as an empty col

        if self.transaction.lmd['type'] == TRANSACTION_LEARN:
            for col_target in self.transaction.lmd['predict_columns']:
                if col_target not in self.transaction.input_data.columns:
                    err = 'Trying to predict column {column} but column not in source data'.format(column=col_target)
                    self.log.error(err)
                    self.transaction.error = True
                    self.transaction.errorMsg = err
                    raise ValueError(err)
                    return

    def run(self):
        # --- Dataset gets randomized or sorted (if timeseries) --- #
        result = self._get_prepared_input_df()
        # --- Dataset gets randomized or sorted (if timeseries) --- #

        # --- Some information about the dataset gets transplanted into transaction level variables --- #
        self.transaction.input_data.columns = result.columns.values.tolist()
        self.transaction.lmd['columns'] = self.transaction.input_data.columns
        self.transaction.input_data.data_frame = result
        # --- Some information about the dataset gets transplanted into transaction level variables --- #

        # --- Some preliminary dataset integrity checks --- #
        self._validate_input_data_integrity()
        # --- Some preliminary dataset integrity checks --- #
