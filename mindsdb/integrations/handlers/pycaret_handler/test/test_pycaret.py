import importlib
import time
from unittest.mock import patch
import pandas as pd
import pytest

from mindsdb_sql_parser import parse_sql
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
            parse_sql(sql)
        )
        assert ret.error_code is None
        if ret.data is not None:
            return ret.data.to_df()

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_classifier(self, mock_handler):
        df = pd.DataFrame({
            'sepal_length': [5.1, 4.9, 4.7, 4.6, 6.4, 6.9, 5.5, 6.5, 7.7, 6.3, 6.7, 7.2],
            'sepal_width': [3.5, 3.0, 3.2, 3.1, 3.2, 3.1, 2.3, 2.8, 2.8, 2.7, 3.3, 3.2],
            'petal_length': [1.4, 4.0, 1.3, 1.5, 4.5, 4.9, 4.0, 4.6, 6.7, 4.9, 5.7, 6.0],
            'petal_width': [0.2, 0.2, 0.2, 0.2, 1.5, 1.5, 1.3, 1.5, 2.0, 1.8, 2.1, 1.8],
            'species': ['Iris-setosa', 'Iris-setosa', 'Iris-setosa', 'Iris-setosa', 'Iris-versicolor', 'Iris-versicolor', 'Iris-versicolor', 'Iris-versicolor', 'Iris-virginica', 'Iris-virginica', 'Iris-virginica', 'Iris-virginica']
        })
        self.set_handler(mock_handler, name='pg', tables={'iris': df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            CREATE MODEL proj.my_pycaret_class_model
            FROM pg
                (SELECT sepal_length, sepal_width, petal_length, petal_width, species FROM iris)
            PREDICT species
            USING
              engine = 'pycaret',
              model_type = 'classification',
              model_name = 'xgboost',
              setup_session_id = 123,
              setup_fold = 2;
        ''')
        self.wait_predictor('proj', 'my_pycaret_class_model')

        # run predict
        ret = self.run_sql('''
            SELECT prediction_label
            FROM pg.iris as t
            JOIN proj.my_pycaret_class_model AS m;
        ''')

        assert ret['prediction_label'].iloc[0] == 'Iris-setosa'

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_regression(self, mock_handler):
        df = pd.DataFrame({
            'age': [19, 18, 28, 33, 32, 31, 46, 37],
            'sex': ['female', 'male', 'male', 'male', 'male', 'female', 'female', 'female'],
            'bmi': [27.9, 33.77, 33, 22.705, 28.88, 25.74, 33.44, 27.74],
            'children': [0, 1, 3, 0, 0, 0, 1, 3],
            'smoker': ['yes', 'no', 'no', 'no', 'no', 'no', 'no', 'no'],
            'region': ['southwest', 'southeast', 'southeast', 'northwest', 'northwest', 'southeast', 'southeast', 'northwest'],
            'charges': [16884.924, 1725.5523, 4449.462, 21984.47061, 3866.8552, 3756.6216, 8240.5896, 7281.5056]
        })

        self.set_handler(mock_handler, name='pg', tables={'insurance': df})

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
              setup_session_id = 123,
              setup_fold = 2;
        ''')
        self.wait_predictor('proj', 'my_pycaret_regr_model')

        # run predict
        ret = self.run_sql('''
            SELECT prediction_label
            FROM pg.insurance as t
            JOIN proj.my_pycaret_regr_model AS m;
        ''')

        assert int(ret['prediction_label'].iloc[0]) == 3822

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    @pytest.mark.skip(reason="MindsDB recognizes 'Anomaly' as a keyword so it fails to fetch Anomaly column")
    def test_anomaly(self, mock_handler):
        df = pd.DataFrame({
            'Col1': [0.263995357, 0.764928588, 0.13842355, 0.935242061, 0.605866573, 0.518789697, 0.912225161, 0.608234451, 0.723781923, 0.73359095],
            'Col2': [0.546092303, 0.65397459, 0.065575135, 0.227771913, 0.845269445, 0.837065879, 0.272378939, 0.331678698, 0.429296975, 0.367422001],
            'Col3': [0.336714104, 0.538842451, 0.192801069, 0.553562822, 0.074514511, 0.332993162, 0.365792205, 0.861309323, 0.899016587, 0.088600152],
            'Col4': [0.092107835, 0.995016662, 0.014465045, 0.176370646, 0.241530075, 0.514723634, 0.562208164, 0.158963258, 0.073715215, 0.208463224],
            'Col5': [0.325261175, 0.805967636, 0.957033424, 0.331664957, 0.307923366, 0.355314772, 0.50189852, 0.558449452, 0.885169295, 0.182754409],
            'Col6': [0.212464853, 0.780304761, 0.458443656, 0.634508561, 0.373030452, 0.465650668, 0.413997158, 0.013080054, 0.570250227, 0.736672363],
            'Col7': [0.258565714, 0.437317789, 0.559647989, 0.109202597, 0.994553306, 0.896994183, 0.488468506, 0.251942977, 0.017265143, 0.538513303],
            'Col8': [0.869236755, 0.277978893, 0.42307639, 0.11247202, 0.183727053, 0.034959735, 0.111113968, 0.249329646, 0.550683376, 0.049843054],
            'Col9': [0.197077957, 0.843918225, 0.24339588, 0.281278233, 0.329148141, 0.73458152, 0.191947043, 0.927804425, 0.71326865, 0.891548497],
            'Col10': [0.292984504, 0.70343162, 0.43962138, 0.107867968, 0.922947409, 0.25345779, 0.29565178, 0.355286799, 0.980911322, 0.308864217]
        })

        self.set_handler(mock_handler, name='pg', tables={'anomaly': df})

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
              setup_session_id = 123,
              setup_fold = 2;
        ''')
        self.wait_predictor('proj', 'my_pycaret_anom_model')

        # run predict
        # TODO: is there a workaround for this? (it works when ran in web UI)
        ret = self.run_sql('''
            SELECT m.Anomaly
            FROM pg.anomaly as t
            JOIN proj.my_pycaret_anom_model AS m;
        ''')

        assert int(ret['Anomaly'].iloc[0]) == 0

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_cluster(self, mock_handler):
        df = pd.DataFrame({
            'Age': [58, 59, 62, 59, 87, 29, 54, 87],
            'Income': [77769, 81799, 74751, 74373, 17760, 13157, 76500, 42592],
            'SpendingScore': [0.7913287771988531, 0.7910820467274178, 0.7026569520102857, 0.7656795619984281, 0.3487775484305076, 0.8470341025128374, 0.7851978501165687, 0.3552896820382753],
            'Savings': [6559.8299230048315, 5417.661426197439, 9258.992965034067, 7346.334503537976, 16869.507130301474, 3535.5143522162816, 6878.884248553975, 18086.287157859304]
        })

        self.set_handler(mock_handler, name='pg', tables={'jewellery': df})

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

        assert ret['Cluster'].iloc[0] == "Cluster 0"

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_timeseries(self, mock_handler):
        df = pd.DataFrame({
            'Year': [1949, 1949, 1949, 1949, 1949, 1949, 1949, 1949, 1949, 1949, 1949, 1949, 1950, 1950, 1950, 1950, 1950, 1950, 1950, 1950],
            'Month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8],
            'Passengers': [112, 118, 132, 129, 121, 135, 148, 148, 136, 119, 104, 118, 115, 126, 141, 135, 125, 149, 170, 170]
        })

        self.set_handler(mock_handler, name='pg', tables={'airline': df})

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

        assert int(ret['y_pred'].iloc[0]) == 125
