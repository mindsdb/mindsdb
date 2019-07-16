import os
from dateutil.parser import parse as parse_datetime

from mindsdb.libs.constants.mindsdb import *
from mindsdb.config import *

import pandas as pd
import lightwood


class LightwoodBackend():

    def __init__(self, transaction):
        self.transaction = transaction
        self.predictor = None

    def _get_group_by_key(self, group_by, row):
        gb_lookup_key = '!!@@!!'
        for column in group_by:
            gb_lookup_key += column + '_' + row[column] + '!!@@!!'
        return gb_lookup_key

    def _create_timeseries_df(self, original_df):
        group_by = self.transaction.lmd['model_group_by']
        order_by = [x[0] for x in self.transaction.lmd['model_order_by']]
        nr_samples = self.transaction.lmd['window_size']

        group_by_ts_map = {}

        for _, row in original_df.iterrows():
            gb_lookup_key = self._get_group_by_key(group_by, row)
            if gb_lookup_key not in group_by_ts_map:
                group_by_ts_map[gb_lookup_key] = []

            for col in order_by:
                try:
                    row[col] = float(row[col])
                except:
                    try:
                        row[col] = float(parse_datetime(row[col]).timestamp())
                    except:
                        self.transaction.log.error(f'Backend Lightwood does not support ordering by the column: {col} !, Faulty value: {row[col]}')
                        exit()

            group_by_ts_map[gb_lookup_key].append(row)

        for k in group_by_ts_map:
            group_by_ts_map[k] = pd.DataFrame.from_records(group_by_ts_map[k], columns=original_df.columns)
            group_by_ts_map[k] = group_by_ts_map[k].sort_values(by=order_by)

            for order_col in order_by:
                for i in range(0,len(group_by_ts_map[k])):
                    group_by_ts_map[k][order_col] = group_by_ts_map[k][order_col].astype(object)

                    numerical_value = float(group_by_ts_map[k][order_col].iloc[i])
                    arr_val = [str(numerical_value)]

                    group_by_ts_map[k][order_col].iat[i] = arr_val

                    previous_indexes = list(range(i - nr_samples, i))
                    previous_indexes = [x for x in previous_indexes if x >= 0]
                    previous_indexes.reverse()

                    for prev_i in previous_indexes:
                        group_by_ts_map[k].iloc[i][order_col].append(group_by_ts_map[k].iloc[prev_i][order_col][-1])

                    group_by_ts_map[k].iloc[i][order_col].reverse()
                    group_by_ts_map[k][order_col].iat[i] = ' '.join(group_by_ts_map[k].iloc[i][order_col])

        combined_df = pd.concat(list(group_by_ts_map.values()))
        return combined_df

    def _create_lightwood_config(self):
        config = {}

        #config['name'] = 'lightwood_predictor_' + self.transaction.lmd['name']

        config['input_features'] = []
        config['output_features'] = []

        for col_name in self.transaction.input_data.columns:
            if col_name in self.transaction.lmd['malformed_columns']['names']:
                continue

            col_stats = self.transaction.lmd['column_stats'][col_name]
            data_subtype = col_stats['data_subtype']
            data_type = col_stats['data_type']

            lightwood_data_type = None

            other_keys = {'encoder_attrs': {}}
            if data_type in (DATA_TYPES.NUMERIC):
                lightwood_data_type = 'numeric'

            elif data_type in (DATA_TYPES.CATEGORICAL):
                lightwood_data_type = 'categorical'

            elif data_subtype in (DATA_SUBTYPES.DATE):
                lightwood_data_type = 'categorical'

            elif data_subtype in (DATA_SUBTYPES.TIMESTAMP):
                lightwood_data_type = 'datetime'

            elif data_subtype in (DATA_SUBTYPES.IMAGE):
                lightwood_data_type = 'image'
                other_keys['encoder_attrs']['aim'] = 'balance'

            elif data_subtype in (DATA_SUBTYPES.TEXT):
                lightwood_data_type = 'text'

            # @TODO Handle lightwood's time_series data type

            else:
                self.transaction.log.error(f'The lightwood model backend is unable to handle data of type {data_type} and subtype {data_subtype} !')
                raise Exception('Failed to build data definition for Lightwood model backend')

            col_config = {
                'name': col_name,
                'type': lightwood_data_type
            }
            col_config.update(other_keys)

            if col_name not in self.transaction.lmd['predict_columns']:
                config['input_features'].append(col_config)
            else:
                config['output_features'].append(col_config)

        return config

    def train(self):
        if self.transaction.lmd['model_order_by'] is not None and len(self.transaction.lmd['model_order_by']) > 0:
            train_df = self._create_timeseries_df(self.transaction.input_data.train_df)
            test_df = self._create_timeseries_df(self.transaction.input_data.test_df)
        else:
            train_df = self.transaction.input_data.train_df
            test_df = self.transaction.input_data.test_df

        lightwood_config = self._create_lightwood_config()
        
        if self.transaction.lmd['skip_model_training'] == True:
            self.predictor = lightwood.Predictor(load_from_path=os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.transaction.lmd['name'] + '_lightwood_data'))
        else:
            self.predictor = lightwood.Predictor(lightwood_config)
            self.predictor.learn(from_data=train_df, test_data=test_df)
            self.transaction.log.info('Training accuracy of: {}'.format(self.predictor.train_accuracy))

        self.transaction.lmd['lightwood_data']['save_path'] = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.transaction.lmd['name'] + '_lightwood_data')
        self.predictor.save(path_to=self.transaction.lmd['lightwood_data']['save_path'])

    def predict(self, mode='predict', ignore_columns=[]):
        if mode == 'predict':
            # Doing it here since currently data cleanup is included in this, in the future separate data cleanup
            lightwood_config = self._create_lightwood_config()
            df = self.transaction.input_data.data_frame
        if mode == 'validate':
            df = self.transaction.input_data.validation_df
        elif mode == 'test':
            df = self.transaction.input_data.test_df

        if self.transaction.lmd['model_order_by'] is not None and len(self.transaction.lmd['model_order_by']) > 0:
            df = self._create_timeseries_df(df)

        if self.predictor is None:
            self.predictor = lightwood.Predictor(load_from_path=self.transaction.lmd['lightwood_data']['save_path'])

        # not the most efficient but least prone to bug and should be fast enough
        if len(ignore_columns)  > 0:
            run_df = df.copy(deep=True)
            for col_name in ignore_columns:
                run_df[col_name] = [None] * len(run_df[col_name])
        else:
            run_df = df

        predictions = self.predictor.predict(when_data=run_df)

        formated_predictions = {}
        for k in predictions:
            formated_predictions[k] = predictions[k]['predictions']

        return formated_predictions
