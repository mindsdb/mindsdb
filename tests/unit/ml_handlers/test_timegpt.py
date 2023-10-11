import os
import time
import pytest
from unittest.mock import patch

import pandas as pd

from mindsdb_sql import parse_sql

from ..executor_test_base import BaseExecutorTest
# from mindsdb.integrations.handlers.timegpt_handler.timegpt_handler import TimeGPTHandler


TIME_GPT_API_KEY = os.environ.get("TIME_GPT_API_KEY")
os.environ["TIMEGPT_API_KEY"] = TIME_GPT_API_KEY


class TestTimeGPT(BaseExecutorTest):
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
                    raise RuntimeError("predictor failed", ret["ERROR"][0])
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    def test_missing_required_keys(self):
        # create project
        self.run_sql("create database proj")
        self.run_sql(f"""create ml_engine timegpt from timegpt using api_key='{TIME_GPT_API_KEY}';""")
        # with pytest.raises(Exception):
        self.run_sql(
            """
              create model proj.test_timegpt_missing_required_keys
              predict answer
              using
                engine='timegpt';
           """
        )

    def test_unknown_arguments(self):
        self.run_sql("create database proj")
        self.run_sql(f"""create ml_engine timegpt from timegpt using api_key='{TIME_GPT_API_KEY}';""")
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                create model proj.test_timegpt_unknown_arguments
                predict answer
                using
                    engine='timegpt',
                    api_key='{TIME_GPT_API_KEY}',
                    evidently_wrong_argument='wrong value';  --- this is a wrong argument name
            """
            )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_forecast_group(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv('tests/unit/ml_handlers/data/house_sales.csv')
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(f"""create ml_engine timegpt from timegpt using api_key='{TIME_GPT_API_KEY}';""")

        self.run_sql(
            f"""
           create model proj.test_timegpt_forecast
           predict ma
           order by saledate
           group by type, bedrooms
           window 128
           horizon 5
           using
             engine='timegpt',
             api_key='{TIME_GPT_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_timegpt_forecast")

        self.run_sql(
            """
            SELECT p.ma
            FROM proj.test_timegpt_forecast as p
            JOIN pg.df as t
            WHERE p.saledate > LATEST;
        """
        )

        # asserts
