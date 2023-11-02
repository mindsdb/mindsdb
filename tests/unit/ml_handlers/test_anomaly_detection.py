import time
import pandas as pd
from unittest.mock import patch
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest
from mindsdb.integrations.handlers.anomaly_detection_handler.anomaly_detection_handler import (
    choose_model,
    preprocess_data,
)


def test_choose_model():
    df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
    # If no target is specified, we should use the unsupervised model
    model = choose_model(df)
    assert model.__class__.__name__ == "ECOD"
    # If the size of the dataset is less than the semi_supervised_threshold, we should use the semi-supervised model
    model = choose_model(df, target="class", supervised_threshold=50)
    assert model.__class__.__name__ == "XGBOD"
    # If the model type is specified, we should use that model type and override default logic
    model = choose_model(df, target="class", model_type="supervised", supervised_threshold=50)
    assert model.__class__.__name__ == "CatBoostClassifier"
    # If the size of the dataset is greater than the semi_supervised_threshold, we should use the supervised model
    model = choose_model(df, target="class", supervised_threshold=2)
    assert model.__class__.__name__ == "CatBoostClassifier"
    # If the model type is specified, we should use that model type and override default logic
    model = choose_model(df, target="class", model_type="semi-supervised", supervised_threshold=2)
    assert model.__class__.__name__ == "XGBOD"
    # If the model name is specified, we should use that model name and override default logic
    model = choose_model(df, target=None, model_name="knn", supervised_threshold=2)
    assert model.__class__.__name__ == "KNN"
    # If the model does not exist, we should raise an error
    try:
        model = choose_model(df, target="class", model_name="not_a_model", supervised_threshold=2)
        assert False
    except AssertionError:
        assert True


def test_preprocess_data():
    df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
    preprocessed_df = preprocess_data(df)
    assert len(preprocessed_df) == len(df)


class TestAnomalyDetectionHandler(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_default_supervised_model(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict class
           using
             engine='anomaly_detection',
             type='supervised'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert ret["model_name"][0] == "CatBoostClassifier"

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_non_default_supervised_model(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict class
           using
             engine='anomaly_detection',
             type='supervised',
             model_name='nb'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert ret["model_name"][0] == "GaussianNB"

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_specify_anomaly_type(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict outlier
           using
             engine='anomaly_detection',
             anomaly_type='clustered',
             type='unsupervised'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert ret["model_name"][0] == "PCA"

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_ensemble(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create ANOMALY DETECTION MODEL proj.modelx
           from pg (select * from df)
           using
             engine='anomaly_detection',
             ensemble_models=['pca', 'knn', 'ecod']
        """
        )
        #  change model_names to ensemble_models for clarity
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert all(ret["model_name"] == pd.Series(["PCA", "KNN", "ECOD"]))

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_default_semi_supervised_model(self, mock_handler):
        # create database
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict class
           using
            engine='anomaly_detection',
            type='semi-supervised'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert ret["model_name"][0] == "XGBOD"

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_default_unsupervised_model(self, mock_handler):
        # dataset, string values
        df = pd.read_csv("tests/unit/ml_handlers/data/anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create anomaly detection model proj.modelx
           from pg (select * from df)
           using
           engine='anomaly_detection'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.outlier
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

        ret = self.run_sql(
            """
           describe model proj.modelx.model
        """
        )
        assert ret["model_name"][0] == "ECOD"
