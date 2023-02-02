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

        # mock a time series dataset
        df = pd.read_parquet("tests/unit/ml_handlers/data/time_series_df.parquet")
        df = df[df["unique_id"]=="H1"]
        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # generate ground truth predictions from the package
        prediction_horizon = 4
        sf = StatsForecast(models=[AutoARIMA()], freq="D")
        sf.fit(df)
        forecast_df = sf.predict(prediction_horizon)
        package_predictions = forecast_df.reset_index(drop=True).iloc[:, -1]

        # create project
        self.run_sql('create database proj')

        # create predictor
        self.run_sql(f'''
           create model proj.modelx
           from pg (select * from df)
           predict y
           order by ds
           horizon {prediction_horizon}
           using 
             engine='statsforecast'
        ''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        result_df = self.run_sql('''
           SELECT p.*
           FROM pg.df as t 
           JOIN proj.modelx as p
        ''')

        # check against ground truth
        mindsdb_result = result_df.iloc[:, -1]
        assert len(mindsdb_result) == prediction_horizon
        assert np.allclose(mindsdb_result, package_predictions)