import time
from unittest.mock import patch
import pandas as pd
import pytest
from mindsdb_sql import parse_sql
from ..executor_test_base import BaseExecutorTest


class TestAuto_ts(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

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

    def test_invalid_time_period(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.auto_ts_invalid_time_period
            FROM pg
                (SELECT * FROM df)
            PREDICT Sales
            ORDER BY Time_period
            USING
               engine="auto_ts",
               score_type = 'rmse',
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_interval = 'invalid_time_period';
           """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_time_period")

    def test_invalid_score_type(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.auto_ts_invalid_score_type
            FROM pg
                (SELECT * FROM df)
            PREDICT Sales
            ORDER BY Time_period
            USING
               engine="auto_ts",
               score_type = 'invalid_score_type',
               ts_columns = 'Time_period',
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_period = 'M';
               """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_score_type")

    def test_invalid_non_seasonal_pdq(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.auto_ts_invalid_non_seasonal_pdq
            FROM pg
                (SELECT * FROM df)
            PREDICT Sales
            ORDER BY Time_Period
            USING
               engine="auto_ts",
               score_type = 'rmse',
               non_seasonal_pdq = 'invalid_non_seasonal_pdq',
               seasonal_period = 12,
               time_period = 'M';
               """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_non_seasonal_pdq")

    def test_invalid_model(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.auto_ts_invalid_model
            FROM pg
                (SELECT * FROM df)
            PREDICT Sales
            ORDER BY Time_Period
            USING
               engine="auto_ts",
               score_type = 'rmse',
               model_type = 'invalid_model',
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_period = 'M';
               """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_model")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_handler_predictions(self, mock_handler):
        df = pd.read_csv("unit/ml_handlers/data/sales.csv")
        self.run_sql("create database proj")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.auto_ts
            FROM pg (SELECT * FROM df)
            PREDICT sales
            ORDER BY time_period
            USING
               engine="auto_ts",
               time_period = 'M',
               cv= 5,
               score_type = 'rmse',
               non_seasonal_pdq = 'None',
               seasonal_period = 12;
               """
        )
        self.wait_predictor("proj", "auto_ts")

        original_prediction = 611
        handler_prediction = self.run_sql(
            """
            SELECT sales_preds
            FROM proj.auto_ts
            WHERE time_period = '2013-04-01'
            AND marketing_expense = 256;
            """
        )
        handler_prediction = int(handler_prediction['sales_preds'][0])
        assert (original_prediction - handler_prediction) < 1, f"The handler prediction was {handler_prediction} and the original prediction was {original_prediction}"

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_handler_batch_predictions(self, mock_handler):
        df = pd.read_csv("unit/ml_handlers/data/sales.csv")
        self.run_sql("create database proj")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.auto_ts
            FROM pg (SELECT * FROM df)
            PREDICT sales
            ORDER BY time_period
            USING
               engine="auto_ts",
               time_period = 'M',
               cv= 5,
               score_type = 'rmse',
               non_seasonal_pdq = 'None',
               seasonal_period = 12;
               """
        )
        self.wait_predictor("proj", "auto_ts")

        batch_length = 48
        handler_prediction = self.run_sql(
            """
            SELECT m.sales_preds
            FROM pg.df as t
            JOIN proj.auto_ts as m ;
            """
        )

        handler_prediction = handler_prediction['sales_preds'].to_list()
        assert len(handler_prediction) == batch_length, f"The handler only returned {len(handler_prediction)} predictions, but it should have returned {batch_length} predictions"
