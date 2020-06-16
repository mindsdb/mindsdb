import pytest
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression
from mindsdb.libs.controllers.predictor import Predictor
from mindsdb.libs.data_sources.file_ds import FileDS
from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES

from tests.unit_tests.data_generators import (generate_value_cols,
                                              generate_timeseries_labels,
                                              columns_to_file)
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

    @pytest.mark.slow
    def test_explain_prediction(self):
        mdb = Predictor(name='test_home_rentals')

        mdb.learn(
            from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
            to_predict='rental_price',
            stop_training_in_x_seconds=1
        )

        result = mdb.predict(when={"number_of_rooms": 2, "sqft": 1384})
        explanation = result[0].explain()['rental_price'][0]
        assert explanation['value']
        assert explanation['confidence']
        assert explanation['explanation']
        assert explanation['simple']
        assert explanation['model_result']

    @pytest.mark.skip(reason="Causes error in probabilistic validator")
    @pytest.mark.slow
    def test_custom_backend(self):
        predictor = Predictor(name='custom_model_test_predictor')

        class CustomDTModel():
            def __init__(self):
                self.clf = LinearRegression()
                le = preprocessing.LabelEncoder()

            def set_transaction(self, transaction):
                self.transaction = transaction
                self.output_columns = self.transaction.lmd['predict_columns']
                self.input_columns = [x for x in self.transaction.lmd['columns']
                                      if x not in self.output_columns]
                self.train_df = self.transaction.input_data.train_df
                self.test_dt = train_df = self.transaction.input_data.test_df

            def train(self):
                self.le_arr = {}
                for col in [*self.output_columns, *self.input_columns]:
                    self.le_arr[col] = preprocessing.LabelEncoder()
                    self.le_arr[col].fit(pd.concat(
                        [self.transaction.input_data.train_df,
                         self.transaction.input_data.test_df,
                         self.transaction.input_data.validation_df])[col])

                X = []
                for col in self.input_columns:
                    X.append(self.le_arr[col].transform(
                        self.transaction.input_data.train_df[col]))

                X = np.swapaxes(X, 1, 0)

                # Only works with one output column
                Y = self.le_arr[self.output_columns[0]].transform(
                    self.transaction.input_data.train_df[self.output_columns[0]])

                self.clf.fit(X, Y)

            def predict(self, mode='predict', ignore_columns=[]):
                if mode == 'predict':
                    df = self.transaction.input_data.data_frame
                if mode == 'validate':
                    df = self.transaction.input_data.validation_df
                elif mode == 'test':
                    df = self.transaction.input_data.test_df

                X = []
                for col in self.input_columns:
                    X.append(self.le_arr[col].transform(df[col]))

                X = np.swapaxes(X, 1, 0)

                predictions = self.clf.predict(X)

                formated_predictions = {self.output_columns[0]: predictions}

                return formated_predictions

        dt_model = CustomDTModel()

        predictor.learn(to_predict='rental_price',
                        from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
                        backend=dt_model)
        predictions = predictor.predict(
            when_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",
            backend=dt_model)

        assert predictions

    @pytest.mark.slow
    def test_data_source_setting(self):
        data_url = 'https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/german_credit_data/processed_data/test.csv'
        data_source = FileDS(data_url)
        data_source.set_subtypes({})

        data_source_mod = FileDS(data_url)
        data_source_mod.set_subtypes({'credit_usage': 'Int', 'Average_Credit_Balance': 'Text',
             'existing_credits': 'Binary Category'})

        analysis = Predictor('analyzer1').analyse_dataset(data_source)
        analysis_mod = Predictor('analyzer2').analyse_dataset(data_source_mod)

        a1 = analysis['data_analysis_v2']
        a2 = analysis_mod['data_analysis_v2']
        assert (len(a1) == len(a2))
        assert (a1['over_draft']['typing']['data_type'] ==
                a2['over_draft']['typing']['data_type'])

        assert (a1['credit_usage']['typing']['data_type'] ==
                a2['credit_usage']['typing']['data_type'])
        assert (a1['credit_usage']['typing']['data_subtype'] !=
                a2['credit_usage']['typing']['data_subtype'])
        assert (a2['credit_usage']['typing']['data_subtype'] == DATA_SUBTYPES.INT)

        assert (a1['Average_Credit_Balance']['typing']['data_type'] !=
                a2['Average_Credit_Balance']['typing']['data_type'])
        assert (a1['Average_Credit_Balance']['typing']['data_subtype'] !=
                a2['Average_Credit_Balance']['typing']['data_subtype'])
        assert (a2['Average_Credit_Balance']['typing'][
                    'data_subtype'] == DATA_SUBTYPES.TEXT)
        assert (a2['Average_Credit_Balance']['typing'][
                    'data_type'] == DATA_TYPES.SEQUENTIAL)

        assert (a1['existing_credits']['typing']['data_type'] ==
                a2['existing_credits']['typing']['data_type'])
        assert (a1['existing_credits']['typing']['data_subtype'] !=
                a2['existing_credits']['typing']['data_subtype'])
        assert (a2['existing_credits']['typing'][
                    'data_subtype'] == DATA_SUBTYPES.SINGLE)

    @pytest.mark.skip(reason='Test gets stuck somewhere in lightwood, need investigation')
    @pytest.mark.slow
    def test_timeseries(self, tmp_path):
        ts_hours = 12
        data_len = 120
        train_file_name = os.path.join(str(tmp_path), 'train_data.csv')
        test_file_name = os.path.join(str(tmp_path), 'test_data.csv')

        features = generate_value_cols(['date', 'int'], data_len, ts_hours * 3600)
        labels = [generate_timeseries_labels(features)]

        feature_headers = list(map(lambda col: col[0], features))
        label_headers = list(map(lambda col: col[0], labels))

        # Create the training dataset and save it to a file
        columns_train = list(
            map(lambda col: col[1:int(len(col) * 3 / 4)], features))
        columns_train.extend(
            list(map(lambda col: col[1:int(len(col) * 3 / 4)], labels)))
        columns_to_file(columns_train, train_file_name, headers=[*feature_headers,
                                                                 *label_headers])
        # Create the testing dataset and save it to a file
        columns_test = list(
            map(lambda col: col[int(len(col) * 3 / 4):], features))
        columns_to_file(columns_test, test_file_name, headers=feature_headers)

        mdb = Predictor(name='test_timeseries')

        mdb.learn(
            from_data=train_file_name,
            to_predict=label_headers,
            order_by=feature_headers[0],
            # ,window_size_seconds=ts_hours* 3600 * 1.5
            window_size=3,
            stop_training_in_x_seconds=2
        )

        results = mdb.predict(when_data=test_file_name, use_gpu=False)

        for row in results:
            expect_columns = [label_headers[0],
                              label_headers[0] + '_confidence']
            for col in expect_columns:
                assert col in row

        models = mdb.get_models()
        model_data = mdb.get_model_data(models[0]['name'])
        assert model_data


