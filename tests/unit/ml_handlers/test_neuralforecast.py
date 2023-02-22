import time
from unittest.mock import patch
import numpy as np
import pandas as pd

from mindsdb.integrations.handlers.neuralforecast_handler.neuralforecast_handler import NeuralForecastHandler
from mindsdb_sql import parse_sql


from tests.unit.executor_test_base import BaseExecutorTest


def create_mock_df():
    df2 = pd.DataFrame(pd.date_range(start="1/1/2010", periods=31, freq="Q"), columns=["time_col"])
    df3 = df2.copy()

    df2["target_col"] = range(1, 32)
    df2["group_col"] = "a"
    df2["group_col_2"] = "a2"
    df2["group_col_3"] = "a3"

    df3["target_col"] = range(11, 42)
    df3["group_col"] = "b"
    df3["group_col_2"] = "b2"
    df3["group_col_3"] = "b3"

    return pd.concat([df2, df3]).reset_index(drop=True)


class TestNeuralForecast(BaseExecutorTest):
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
    def test_grouped(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = create_mock_df()
        self.set_handler(mock_handler, name="pg", tables={"df": df})


        # now add more groups
        self.run_sql(
            """
           create model proj.model_multi_group
           from pg (select * from df)
           predict target_col
           order by time_col
           group by group_col, group_col_2, group_col_3
           window 6
           horizon 3
           using
             engine='neuralforecast',
             frequency='Q',
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "model_multi_group")

        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_multi_group as p
           where t.group_col_2='a2'
        """
        )
        assert list(round(result_df["target_col"])) == [32, 33, 34]

        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_multi_group as p
           where t.group_col='b'
        """
        )
        assert list(round(result_df["target_col"])) == [42, 43, 44]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_with_exog_vars(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = create_mock_df()
        df["exog_var_1"] = 5 * df.index
        self.set_handler(mock_handler, name="pg", tables={"df": df})


        # now add more groups
        self.run_sql(
            """
           create model proj.model_exog_var
           from pg (select * from df)
           predict target_col
           order by time_col
           group by group_col, group_col_2, group_col_3
           window 6
           horizon 3
           using
             engine='neuralforecast',
             frequency='Q',
             train_time=0.01,
             exogenous_vars=['exog_var_1']
        """
        )
        self.wait_predictor("proj", "model_multi_group")

        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_exog_var as p
           where t.group_col='b'
        """)
        assert list(round(result_df["target_col"])) == [42, 43, 44]
