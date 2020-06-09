import json
from uuid import uuid4
import pytest
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.phases.type_deductor.type_deductor import TypeDeductor
from .utils import test_column_types


class TestTypeDeductor:
    @pytest.fixture()
    def lmd(self, transaction):
        lmd = transaction.lmd
        lmd['column_stats'] = {}
        lmd['stats_v2'] = {}
        lmd['data_types'] = {}
        lmd['data_subtypes'] = {}
        lmd['data_preparation'] = {}
        lmd['force_categorical_encoding'] = {}
        lmd['columns_to_ignore'] = []
        lmd['sample_margin_of_error'] = 0.005
        lmd['sample_confidence_level'] = 1 - lmd['sample_margin_of_error']
        lmd['handle_text_as_categorical'] = False
        return lmd

    def test_type_deduction(self, transaction, lmd):
        """Tests that basic cases of type deduction work correctly"""
        hmd = transaction.hmd

        type_deductor = TypeDeductor(session=transaction.session,
                                     transaction=transaction)

        n_points = 100

        # Apparently for n_category_values = 10 it doesnt work
        n_category_values = 4
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'numeric_float': np.linspace(0, n_points, n_points),
            'date_timestamp': [(datetime.now() - timedelta(minutes=int(i))).isoformat() for i in range(n_points)],
            'date_date': [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(n_points)],
            'categorical_str': [f'a{x}' for x in (list(range(n_category_values)) * (n_points//n_category_values))],
            'categorical_int': [x for x in (list(range(n_category_values)) * (n_points//n_category_values))],
            'categorical_binary': [0, 1] * (n_points//2),
            'sequential_array': [f"1,2,3,4,5,{i}" for i in range(n_points)],
            'sequential_text': [f'lorem ipsum long text {i}' for i in range(n_points)],
        }, index=list(range(n_points)))

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        type_deductor.run(input_data)

        stats_v2 = lmd['stats_v2']

        for col_name in input_dataframe.columns:
            expected_type = test_column_types[col_name][0]
            expected_subtype = test_column_types[col_name][1]
            assert stats_v2[col_name]['typing']['data_type'] == expected_type
            assert stats_v2[col_name]['typing']['data_subtype'] == expected_subtype
            assert stats_v2[col_name]['typing']['data_type_dist'][expected_type] == 99
            assert stats_v2[col_name]['typing']['data_subtype_dist'][expected_subtype] == 99

        for col_name in stats_v2:
            assert not stats_v2[col_name]['is_foreign_key']

        assert DATA_SUBTYPES.INT in stats_v2['categorical_int']['additional_info']['other_potential_subtypes']
        assert hmd == {}

        assert isinstance(json.dumps(transaction.lmd), str)

    def test_deduce_foreign_key(self, transaction, lmd):
        """Tests that basic cases of type deduction work correctly"""
        hmd = transaction.hmd

        lmd['handle_foreign_keys'] = True

        type_deductor = TypeDeductor(session=transaction.session,
                                     transaction=transaction)
        n_points = 100

        input_dataframe = pd.DataFrame({
            'numeric_id': list(range(n_points)),
            'uuid': [str(uuid4()) for i in range(n_points)]
        }, index=list(range(n_points)))

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        type_deductor.run(input_data)

        stats_v2 = lmd['stats_v2']

        assert stats_v2['numeric_id']['is_foreign_key']
        assert stats_v2['uuid']['is_foreign_key']

        assert 'numeric_id' in lmd['columns_to_ignore']
        assert 'uuid' in lmd['columns_to_ignore']

    def test_empty_column(self, transaction, lmd):
        type_deductor = TypeDeductor(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'empty_column': [None for i in range(n_points)],
        }, index=list(range(n_points)))

        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        type_deductor.run(input_data)

        stats_v2 = lmd['stats_v2']
        assert stats_v2['empty_column']['typing']['data_type'] is None

    def test_empty_values(self, transaction, lmd):
        type_deductor = TypeDeductor(session=transaction.session,
                                    transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_float': np.linspace(0, n_points, n_points),
        }, index=list(range(n_points)))
        input_dataframe['numeric_float'].iloc[::2] = None
        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        type_deductor.run(input_data)

        stats_v2 = lmd['stats_v2']
        assert stats_v2['numeric_float']['typing']['data_type'] == DATA_TYPES.NUMERIC
        assert stats_v2['numeric_float']['typing']['data_subtype'] == DATA_SUBTYPES.FLOAT
        assert stats_v2['numeric_float']['typing']['data_type_dist'][DATA_TYPES.NUMERIC] == 50
        assert stats_v2['numeric_float']['typing']['data_subtype_dist'][DATA_SUBTYPES.FLOAT] == 50

    def test_type_mix(self, transaction, lmd):
        type_deductor = TypeDeductor(session=transaction.session,
                                     transaction=transaction)

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_float': np.linspace(0, n_points, n_points),
        }, index=list(range(n_points)))
        input_dataframe['numeric_float'].iloc[:2] = 'random string'
        input_data = TransactionData()
        input_data.data_frame = input_dataframe
        type_deductor.run(input_data)

        stats_v2 = lmd['stats_v2']
        assert stats_v2['numeric_float']['typing']['data_type'] == DATA_TYPES.NUMERIC
        assert stats_v2['numeric_float']['typing']['data_subtype'] == DATA_SUBTYPES.FLOAT
        assert stats_v2['numeric_float']['typing']['data_type_dist'][DATA_TYPES.NUMERIC] == 97
        assert stats_v2['numeric_float']['typing']['data_subtype_dist'][DATA_SUBTYPES.FLOAT] == 97
