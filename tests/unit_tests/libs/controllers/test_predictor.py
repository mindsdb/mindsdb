import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from mindsdb.libs.controllers.predictor import Predictor
from tests.unit_tests.utils import test_column_types


class TestPredictor:
    def test_analyze_dataset(self):
        predictor = Predictor(name='test')

        n_points = 100
        n_category_values = 4
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'numeric_float': np.linspace(0, n_points, n_points),
            'date_timestamp': [
                (datetime.now() - timedelta(minutes=int(i))).isoformat() for i in
                range(n_points)],
            'date_date': [
                (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in
                range(n_points)],
            'categorical_str': [f'a{x}' for x in (
                    list(range(n_category_values)) * (
                        n_points // n_category_values))],
            'categorical_int': [x for x in (list(range(n_category_values)) * (
                    n_points // n_category_values))],
            'categorical_binary': [0, 1] * (n_points // 2),
            'sequential_array': [f"1,2,3,4,5,{i}" for i in range(n_points)],
            'sequential_text': [f'lorem ipsum long text {i}' for i in
                                range(n_points)],
        }, index=list(range(n_points)))

        model_data = predictor.analyse_dataset(from_data=input_dataframe)
        for col, col_data in model_data['data_analysis_v2'].items():
            expected_type = test_column_types[col][0]
            expected_subtype = test_column_types[col][1]
            assert col_data['typing']['data_type'] == expected_type
            assert col_data['typing']['data_subtype'] == expected_subtype

            assert col_data['empty']
            assert col_data['histogram']
            assert 'percentage_buckets' in col_data
            assert 'nr_warnings' in col_data
            assert not col_data['is_foreign_key']

    def test_analyze_dataset_empty_column(self):
        predictor = Predictor(name='test')

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
            'empty_column': [None for i in range(n_points)]
        }, index=list(range(n_points)))

        model_data = predictor.analyse_dataset(from_data=input_dataframe)

        assert model_data['data_analysis_v2']['empty_column']['empty']['is_empty'] is True

    def test_analyze_dataset_empty_values(self):
        predictor = Predictor(name='test')

        n_points = 100
        input_dataframe = pd.DataFrame({
            'numeric_int': list(range(n_points)),
        }, index=list(range(n_points)))
        input_dataframe['numeric_int'].iloc[::2] = None

        model_data = predictor.analyse_dataset(from_data=input_dataframe)

        assert model_data['data_analysis_v2']['numeric_int']['empty']['empty_percentage'] == 50

    @pytest.mark.skip(reason="too slow")
    def test_explain_prediction(self):
        # @TODO this test needs to use a small dataset, so that it's fast, also needs assertions
        mdb = Predictor(name='test_home_rentals')

        mdb.learn(
            from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
            to_predict='rental_price',
            stop_training_in_x_seconds=6
        )

        # use the model to make predictions
        result = mdb.predict(when={"number_of_rooms": 2, "sqft": 1384})

        result[0].explain()

        when = {"number_of_rooms": 1, "sqft": 384}

        # use the model to make predictions
        result = mdb.predict(when=when)

        result[0].explain()