from collections import defaultdict

import pytest
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.phases.data_analyzer.data_analyzer import DataAnalyzer
from .utils import test_column_types


class TestDataAnalyzer:
    @pytest.fixture()
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

    @pytest.fixture()
    def stats_v2(self):
        result = {}
        for col_name, col_type_pair in test_column_types.items():
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

    @pytest.fixture()
    def stats(self, stats_v2):
        result = defaultdict(dict)
        for k, v in stats_v2.items():
            result[k].update(v['typing'])
        return result

    def test_data_analysis(self, transaction, lmd, stats_v2, stats):
        """Tests that data analyzer doesn't crash on common types"""
        lmd['stats_v2'] = stats_v2
        lmd['column_stats'] = stats
        hmd = transaction.hmd

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
            'sequential_array': [f"1,2,3,4,5,{i}" for i in range(n_points)],
            'sequential_text': [f'lorem ipsum long text {i}' for i in range(n_points)],
        }, index=list(range(n_points)))

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        data_analyzer.run(input_data)

        stats_v2 = lmd['stats_v2']

        for col_name in input_dataframe.columns:
            assert stats_v2[col_name]['empty']['empty_percentage'] == 0
            assert not stats_v2[col_name]['empty']['is_empty']
            assert stats_v2[col_name]['histogram']
            assert 'percentage_buckets' in stats_v2[col_name]
            assert stats_v2[col_name]['bias']['entropy']
            assert stats_v2[col_name]['unique']['unique_values']

        assert stats_v2['categorical_str']['unique']['unique_percentage'] == 4.0
        assert stats_v2['categorical_str']['percentage_buckets'] == [25, 25, 25,
                                                                     25]

        # Assert that the histogram on text field is made using words
        assert isinstance(stats_v2['sequential_text']['histogram']['x'][0], str)

        assert hmd == {}

    def test_empty_values(self, transaction, lmd, stats_v2, stats):
        lmd['stats_v2'] = stats_v2
        lmd['column_stats'] = stats

        data_analyzer = DataAnalyzer(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
        }, index=list(range(n_points)))

        input_dataframe['numeric_int'].iloc[::2] = None
        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        data_analyzer.run(input_data)

        stats_v2 = lmd['stats_v2']

        assert stats_v2['numeric_int']['empty']['empty_percentage'] == 50

    def test_empty_column(self, transaction, lmd, stats_v2, stats):
        lmd['stats_v2'] = stats_v2
        lmd['stats_v2']['empty_column'] = lmd['stats_v2']['numeric_int']
        lmd['column_stats'] = stats
        lmd['column_stats']['empty_column'] = lmd['column_stats']['numeric_int']

        data_analyzer = DataAnalyzer(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'empty_column': [None for i in range(n_points)],
        }, index=list(range(n_points)))

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        data_analyzer.run(input_data)

        stats_v2 = lmd['stats_v2']

        assert stats_v2['empty_column']['empty']['is_empty']

