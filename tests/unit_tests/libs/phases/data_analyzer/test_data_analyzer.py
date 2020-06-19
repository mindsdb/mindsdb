import json

import pytest
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.phases.data_analyzer.data_analyzer import DataAnalyzer
from tests.unit_tests.utils import test_column_types


class TestDataAnalyzer:
    @pytest.fixture(scope='function')
    def lmd(self, transaction):
        lmd = transaction.lmd
        lmd['handle_text_as_categorical'] = False
        lmd['column_stats'] = {}
        lmd['stats_v2'] = {}
        lmd['empty_columns'] = []
        lmd['data_types'] = {}
        lmd['data_subtypes'] = {}
        lmd['data_preparation'] = {}
        lmd['force_categorical_encoding'] = {}
        lmd['columns_to_ignore'] = []
        lmd['sample_margin_of_error'] = 0.005
        lmd['sample_confidence_level'] = 1 - lmd['sample_margin_of_error']
        return lmd

    def get_stats_v2(self, col_names):
        result = {}
        for col_name, col_type_pair in test_column_types.items():
            if col_name in col_names:
                result[col_name] = {
                    'typing': {
                        'data_type': col_type_pair[0],
                        'data_subtype': col_type_pair[1],
                    }
                }

        for k, v in result.items():
            result[k]['typing']['data_type_dist'] = {v['typing']['data_type']: 100}
            result[k]['typing']['data_subtype_dist'] = {v['typing']['data_subtype']: 100}
        return result

    def get_stats(self, stats_v2):
        result = {}
        for col, val in stats_v2.items():
            result[col] = val['typing']
        return result

    def test_data_analysis(self, transaction, lmd):
        """Tests that data analyzer doesn't crash on common types"""
        data_analyzer = DataAnalyzer(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        n_category_values = 4
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'numeric_float': np.linspace(0, n_points, n_points),
            'date_timestamp': [(datetime.now() - timedelta(minutes=int(i))).isoformat() for i in range(n_points)],
            'date_date': [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(n_points)],
            'categorical_str': [f'a{x}' for x in (list(range(n_category_values)) * (n_points//n_category_values))],
            'categorical_binary': [0, 1] * (n_points//2),
            'categorical_int': [x for x in (list(range(n_category_values)) * (n_points // n_category_values))],
            'sequential_array': [f"1,2,3,4,5,{i}" for i in range(n_points)],
            'sequential_text': [f'lorem ipsum long text {i}' for i in range(n_points)],
        }, index=list(range(n_points)))

        stats_v2 = self.get_stats_v2(input_dataframe.columns)
        stats = self.get_stats(stats_v2)
        lmd['stats_v2'] = stats_v2
        lmd['column_stats'] = stats
        hmd = transaction.hmd

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        input_data.sample_df = input_dataframe.iloc[n_points // 2:]
        data_analyzer.run(input_data)

        stats_v2 = lmd['stats_v2']

        for col_name in input_dataframe.columns:
            assert stats_v2[col_name]['empty']['empty_percentage'] == 0
            assert not stats_v2[col_name]['empty']['is_empty']
            assert stats_v2[col_name]['histogram']
            assert 'percentage_buckets' in stats_v2[col_name]
            assert stats_v2[col_name]['bias']['entropy']

        assert stats_v2['categorical_str']['unique']['unique_values']
        assert stats_v2['categorical_str']['unique']['unique_percentage'] == 4.0

        # Assert that the histogram on text field is made using words
        assert isinstance(stats_v2['sequential_text']['histogram']['x'][0], str)

        assert hmd == {}

        assert isinstance(json.dumps(transaction.lmd), str)

    def test_empty_values(self, transaction, lmd):
        data_analyzer = DataAnalyzer(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
        }, index=list(range(n_points)))

        stats_v2 = self.get_stats_v2(input_dataframe.columns)
        stats = self.get_stats(stats_v2)
        lmd['stats_v2'] = stats_v2
        lmd['column_stats'] = stats

        input_dataframe['numeric_int'].iloc[::2] = None
        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        input_data.sample_df = input_dataframe.iloc[n_points // 2:]
        data_analyzer.run(input_data)

        stats_v2 = lmd['stats_v2']

        assert stats_v2['numeric_int']['empty']['empty_percentage'] == 50