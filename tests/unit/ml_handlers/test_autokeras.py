import time
from unittest.mock import patch
import pandas as pd
import numpy as np

from mindsdb_sql import parse_sql


from mindsdb.integrations.handlers.autokeras_handler.autokeras_handler import (
    format_categorical_preds,
)
from tests.unit.executor_test_base import BaseExecutorTest


def test_format_categorical_preds():
    """Tests helper function to put categorical predictions into the right format"""
    predictions = np.array([[0.9, 0.05, 0.05], [0, 1, 0], [0, 0, 1]])
    original_y = pd.Series(["a", "b", "c"])
    keras_output_df = pd.DataFrame({"target": predictions.tolist()})
    formatted_df = format_categorical_preds(predictions, original_y, keras_output_df, "target")
    assert formatted_df["target"].tolist() == ["a", "b", "c"]
    assert formatted_df["confidence"].tolist() == [max(row) for row in predictions]


class TestAutokeras(BaseExecutorTest):
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
            return ret.data.to_df()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_with_numerical_training(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict c
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT *
           FROM proj.modelx
           WHERE a=1
           AND b=25;
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        assert ret.columns.tolist() == ["a", "b", "c"]
        assert len(ret) == 1
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_with_categorical_training(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict c
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT c
           FROM proj.modelx
           WHERE a=1 AND b=25 AND d="odd"
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_with_nulls_in_training_data(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")
        # Make it look like we have missing data
        df["a"][10] = None
        df["b"][25] = np.nan
        df["d"][31] = ""

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict c
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT c
           FROM proj.modelx
           WHERE a=1 AND b=25 AND d="odd";
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_with_bulk_predict_query(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict c
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
            SELECT m.*
            FROM pg.df as t
            JOIN proj.modelx as m
            where t.b>25
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_error_on_predict_query_missing_cols(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict c
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        try:
            # run predict but missing column d in the WHERE clause
            _ = self.run_sql(
                """
            SELECT c
            FROM proj.modelx
            WHERE a=1
            AND b=2;
            """
            )
            assert False
        except Exception:
            assert True

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_with_numerical_training(self, mock_handler):
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict d
           using
             engine='autokeras',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT d
           FROM proj.modelx
           WHERE a=1 AND b=25 AND c=10
        """
        )
        assert ret.d[0] in ["even", "odd"]
