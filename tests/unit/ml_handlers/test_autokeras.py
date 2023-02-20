import time
from unittest.mock import patch
import pandas as pd
import numpy as np
import pytest

from mindsdb_sql import parse_sql


from tests.unit.executor_test_base import BaseExecutorTest


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
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT c
           FROM proj.modelx
           WHERE a=1;
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        # value is around 1
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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT c
           FROM proj.modelx
           WHERE a=1
           AND d="odd";
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        # value is around 1
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_with_nulls_in_training_data(self, mock_handler):

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["c"] = round((df["a"] * 3 + df["b"]) / 50)
        df["d"] = np.where(df.index % 2, "even", "odd")
        # Make it look like we have missing data
        df["a"][10] = np.nan
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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT c
           FROM proj.modelx
           WHERE a=1
           AND d="odd";
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        # value is around 1
        assert (avg_c > -5) and (avg_c < 5)

    @pytest.mark.skip("Can't figure out how to assert it")
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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
            SELECT t.c, m.c, t.a, t.b
            FROM pg.df as t 
            JOIN proj.modelx as m limit 10;
        """
        )
        avg_c = pd.to_numeric(ret.c).mean()
        # value is around 1
        assert (avg_c > -5) and (avg_c < 5)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_error_on_predict_query_too_strict(self, mock_handler):

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        try:
            # run predict
            ret = self.run_sql(
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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT d
           FROM proj.modelx
           WHERE a=1;
        """
        )
        assert ret.d[0][0] in ["even", "odd"]


    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_with_categorical_training(self, mock_handler):

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["d"] = np.where(df.index % 2, "even", "odd")
        df["e"] = np.where(df.index % 3, "not_prime", "kinda_prime")

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT d
           FROM proj.modelx
           WHERE a=1
           AND e="kinda_prime";
        """
        )

        assert ret.d[0][0] in ["even", "odd"]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_with_nulls_in_training_data(self, mock_handler):

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["d"] = np.where(df.index % 2, "even", "odd")
        df["e"] = np.where(df.index % 3, "not_prime", "kinda_prime")
        # Make it look like we have missing data
        df["a"][10] = np.nan
        df["b"][25] = np.nan
        df["e"][31] = ""

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT d
           FROM proj.modelx
           WHERE a=1
           AND e="kinda_prime";
        """
        )

        assert ret.d[0][0] in ["even", "odd"]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_error_on_predict_query_too_strict(self, mock_handler):

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=["a"])
        df["b"] = 50 - df.a
        df["d"] = np.where(df.index % 2, "even", "odd")
        df["e"] = np.where(df.index % 3, "not_prime", "kinda_prime")

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        try:
            # run predict
            ret = self.run_sql(
                """
            SELECT d
            FROM proj.modelx
            WHERE a=1
            AND b=2;
            """
            )
            assert False
        except Exception:
            assert True

    @pytest.mark.skip("Can't figure out how to assert it")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_with_bulk_predict_query(self, mock_handler):

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
             engine='autokeras';
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
            SELECT t.d, m.d, t.a, t.b
            FROM pg.df as t 
            JOIN proj.modelx as m limit 10;
        """
        )

        for i, row in enumerate(ret.d):
            assert ret.d[i][0] in ["even", "odd"]
