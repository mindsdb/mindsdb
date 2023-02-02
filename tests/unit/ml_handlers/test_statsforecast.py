import time
from unittest.mock import patch
import numpy as np
import pandas as pd

from statsforecast.models import AutoARIMA
from statsforecast import StatsForecast
from mindsdb_sql import parse_sql


from tests.unit.executor_test_base import BaseExecutorTest


class TestStatsForecast(BaseExecutorTest):

    def wait_predictor(self, project, name):
        # wait
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
    def test_simple(self, mock_handler):

        # mock a dataset
        n_periods = 50
        date_series = pd.date_range(start=pd.to_datetime("today"), periods=n_periods)
        df = pd.DataFrame({"unique_id": "A", "ds":date_series, "target_series": range(0, n_periods)})

        # 
        sf = StatsForecast(models=[AutoARIMA()], freq="D")
        sf.fit(df)
        forecast_df = sf.predict(h=12)
        ground_truth = forecast_df.reset_index().AutoARIMA.mean()

        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # create project
        self.run_sql('create database proj')

        # create predictor
        self.run_sql('''
           create model proj.modelx
           from pg (select * from df)
           predict target_series
           using 
             engine='statsforecast'
        ''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        ret = self.run_sql('''
           SELECT p.*
           FROM pg.df as t 
           JOIN proj.modelx as p
           where t.target_series>0
        ''')

        # check against ground truth
        mindsdb_result = pd.to_numeric(ret.AutoARIMA).mean()
        assert mindsdb_result == ground_truth