import time

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

    def test_missing_ts_column(self):
        # create project
        self.run_sql("create database proj")

        self.run_sql(
            """
                CREATE MODEL proj.auto_ts_missing_ts_column
                FROM files
                    (SELECT * FROM Sales_and_Marketing)
                PREDICT Sales
                USING
                   engine="auto_ts"
                   score_type = 'rmse',
                   non_seasonal_pdq = 'None',
                   seasonal_period = 12;
                   """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_missing_ts_column")

    def test_invalid_time_period(self):
            # create project
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            CREATE MODEL proj.auto_ts_invalid_time_period
            FROM files
                (SELECT * FROM Sales_and_Marketing)
            PREDICT Sales
            USING
               engine="auto_ts"
               score_type = 'rmse',
               ts_columns = 'Time_period'
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_period = 'invalid_time_period';
           """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_time_period")


    def test_invalid_score_type(self):
        self.run_sql("create database proj")
        self.run_sql(
            f"""
                    CREATE MODEL proj.auto_ts_missing_ts_column
                    FROM files
                        (SELECT * FROM Sales_and_Marketing)
                    PREDICT Sales
                    USING
                       engine="auto_ts"
                       score_type = 'invalid_score_type',
                       ts_columns = 'Time_period'
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
                FROM files
                    (SELECT * FROM Sales_and_Marketing)
                PREDICT Sales
                USING
                   engine="auto_ts"
                   score_type = 'rmse',
                   ts_columns = 'Time_period'
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
            FROM files
                (SELECT * FROM Sales_and_Marketing)
            PREDICT Sales
            USING
               engine="auto_ts"
               score_type = 'rmse',
               ts_columns = 'Time_period'
               model = 'invalid_model',
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_period = 'M';
               """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_invalid_model")

    def test_missing_target(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.auto_ts_missing_target
            FROM files
                (SELECT * FROM Sales_and_Marketing)
            PREDICT Sales
            USING
               engine="auto_ts"
               score_type = 'rmse',
               ts_columns = 'Time_period'
               non_seasonal_pdq = 'None',
               seasonal_period = 12,
               time_period = 'M';
               """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "auto_ts_missing_target")