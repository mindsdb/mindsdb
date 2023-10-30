import importlib
import time
from unittest.mock import patch
import pandas as pd
import pytest

from mindsdb_sql import parse_sql
from tests.unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("pycaret")
    PYCARET_INSTALLED = True
except ImportError:
    PYCARET_INSTALLED = False


@pytest.mark.skipif(not PYCARET_INSTALLED, reason="pycaret is not installed")
class TestPyCaret(BaseExecutorTest):

    def wait_predictor(self, project, name):
        done = False
        for attempt in range(200):
            ret = self.run_sql(
                f"select * from {project}.models where name='{name}'"
            )
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_classifier(self, mock_handler):
        iris_df = pd.read_csv('tests/unit/ml_handlers/data/iris.csv')
        self.set_handler(mock_handler, name='pg', tables={'iris': iris_df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_class_model
            FROM pg
                (SELECT SepalLengthCm, SepalWidthCm, PetalLengthCm, PetalWidthCm, Species FROM iris)
            PREDICT Species
            USING
              engine = 'pycaret',
              model_type = 'classification',
              model_name = 'xgboost',
              setup_session_id = 123;
        ''')
        self.wait_predictor('proj', 'my_pycaret_class_model')

        # run predict
        ret = self.run_sql('''
            SELECT prediction_label
            FROM pg.iris as t
            JOIN proj.my_pycaret_class_model AS m;
        ''')

        # rough estimate from testing
        assert ret['prediction_label'].iloc[0] == 'Iris-setosa'

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_regression(self, mock_handler):
        insurance_df = pd.read_csv('tests/unit/ml_handlers/data/insurance.csv')
        self.set_handler(mock_handler, name='pg', tables={'insurance': insurance_df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_regr_model
            FROM pg
                (SELECT age, sex, bmi, children, smoker, region, charges FROM insurance)
            PREDICT charges
            USING
              engine = 'pycaret',
              model_type = 'regression',
              model_name = 'xgboost',
              setup_session_id = 123;
        ''')
        self.wait_predictor('proj', 'my_pycaret_regr_model')

        # run predict
        ret = self.run_sql('''
            SELECT prediction_label
            FROM pg.insurance as t
            JOIN proj.my_pycaret_regr_model AS m;
        ''')

        # rough estimate from testing
        assert int(ret['prediction_label'].iloc[0]) > 16000

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    @pytest.mark.skip(reason="MindsDB recognizes 'Anomaly' as a keyword so it fails to fetch Anomaly column")
    def test_anomaly(self, mock_handler):
        anomaly_df = pd.read_csv('tests/unit/ml_handlers/data/anomaly.csv')
        self.set_handler(mock_handler, name='pg', tables={'anomaly': anomaly_df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_anom_model
            FROM pg
                (SELECT Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9, Col10 FROM anomaly)
            PREDICT Col10
            USING
              engine = 'pycaret',
              model_type = 'anomaly',
              model_name = 'iforest',
              setup_session_id = 123;

        ''')
        self.wait_predictor('proj', 'my_pycaret_anom_model')

        # run predict
        # TODO: is there a workaround for this? (it works when ran in web UI)
        ret = self.run_sql('''
            SELECT m.Anomaly
            FROM pg.anomaly as t
            JOIN proj.my_pycaret_anom_model AS m;
        ''')

        # rough estimate from testing
        assert int(ret['Anomaly'].iloc[0]) == 0

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_cluster(self, mock_handler):
        jewellery_df = pd.read_csv('tests/unit/ml_handlers/data/jewellery.csv')
        self.set_handler(mock_handler, name='pg', tables={'jewellery': jewellery_df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_cluster_model
            FROM pg
                (SELECT Age, Income, SpendingScore, Savings FROM jewellery)
            PREDICT Savings
            USING
              engine = 'pycaret',
              model_type = 'clustering',
              model_name = 'kmeans',
              setup_session_id = 123;
        ''')
        self.wait_predictor('proj', 'my_pycaret_cluster_model')

        # run predict
        ret = self.run_sql('''
            SELECT m.Cluster
            FROM pg.jewellery as t
            JOIN proj.my_pycaret_cluster_model AS m;
        ''')

        # rough estimate from testing
        assert ret['Cluster'].iloc[0] == "Cluster 2"

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_timeseries(self, mock_handler):
        airline_df = pd.read_csv('tests/unit/ml_handlers/data/airline.csv')
        self.set_handler(mock_handler, name='pg', tables={'airline': airline_df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_timeseries_model
            FROM pg
                (SELECT Year, Month, Passengers FROM airline)
            PREDICT Passengers
            USING
              engine = 'pycaret',
              model_type = 'time_series',
              model_name = 'naive',
              setup_fh = 3,
              predict_fh = 36,
              setup_session_id = 123;
        ''')
        self.wait_predictor('proj', 'my_pycaret_timeseries_model')

        # run predict
        ret = self.run_sql('''
            SELECT m.y_pred
            FROM pg.airline as t
            JOIN proj.my_pycaret_timeseries_model AS m;
        ''')

        # rough estimate from testing
        assert int(ret['y_pred'].iloc[0]) > 500
