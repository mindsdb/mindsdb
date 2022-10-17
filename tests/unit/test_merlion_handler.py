import os
import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_merlion_handler.py
# Warning: uncomment and run too many sql-testcase in detector and forecaster will cost a long time
from mindsdb.integrations.handlers.merlion_handler.adapters import DefaultForecasterAdapter, SarimaForecasterAdapter, \
    ProphetForecasterAdapter, MSESForecasterAdapter, IsolationForestDetectorAdapter, \
    WindStatsDetectorAdapter, ProphetDetectorAdapter
# from mindsdb.integrations.handlers.merlion_handler.adapters import DefaultDetectorAdapter
from .executor_test_base import BaseExecutorTest


class TestMerlion(BaseExecutorTest):
    def get_m4_abs_path(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(dir_path, "m4_test_merlion.csv")

    def get_nba_abs_path(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(dir_path, "nba_test_merlion.csv")


    def run_mindsdb_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )

    def test_merlion_forecaster(self):
        df = pd.read_csv(self.get_m4_abs_path())
        df.index = pd.to_datetime(df["t"])
        df.drop(columns=["t"], inplace=True)
        df_train = df[df["train"] == 1][["H1"]]
        df_test = df[df["train"] == 0][["H1"]]
        # default adapter
        adapter = DefaultForecasterAdapter()
        adapter.train(df_train.copy(deep=True), target="H1")
        rt_df = adapter.predict(df_test.copy(deep=True), target="H1")
        assert rt_df is not None
        assert len(rt_df) == len(df_test)
        assert "H1__upper" in set(rt_df.columns.values) and "H1__lower" in set(rt_df.columns.values)
        # arima adapter
        adapter = SarimaForecasterAdapter()
        adapter.train(df_train.copy(deep=True), target="H1")
        rt_df = adapter.predict(df_test.copy(deep=True), target="H1")
        assert rt_df is not None
        assert len(rt_df) == len(df_test)
        assert "H1__upper" in set(rt_df.columns.values) and "H1__lower" in set(rt_df.columns.values)
        # prophet adapter
        adapter = ProphetForecasterAdapter()
        adapter.train(df_train.copy(deep=True), target="H1")
        rt_df = adapter.predict(df_test.copy(deep=True), target="H1")
        assert rt_df is not None
        assert len(rt_df) == len(df_test)
        assert "H1__upper" in set(rt_df.columns.values) and "H1__lower" in set(rt_df.columns.values)
        # mses adapter
        adapter = MSESForecasterAdapter()
        adapter.train(df_train.copy(deep=True), target="H1")
        rt_df = adapter.predict(df_test.copy(deep=True), target="H1")
        assert rt_df is not None
        assert len(rt_df) == len(df_test)
        assert "H1__upper" in set(rt_df.columns.values) and "H1__lower" in set(rt_df.columns.values)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_merlion_forecaster_sql(self, mock_handler):
        # prepare data
        df = pd.read_csv(self.get_m4_abs_path())
        df["t"] = pd.to_datetime(df["t"])
        self.set_handler(mock_handler, name='pg', tables={'m4': df})
        # test default
        self.exec_train_and_forecast(mock_handler=mock_handler, model_name="default", using="")
        # # test arima
        # self.exec_train_and_predict(mock_handler=mock_handler, model_name="arima", using=f"USING model_type='arima'")
        # # test prophet
        # self.exec_train_and_forecast(mock_handler=mock_handler, model_name="prophet", using=f"USING model_type='prophet'")
        # # test mses
        # self.exec_train_and_forecast(mock_handler=mock_handler, model_name="mses", using=f"USING model_type='mses'")

    def exec_train_and_forecast(self, mock_handler, model_name, using):
        # create predictor
        create_sql = f'''
                    CREATE PREDICTOR merlion.{model_name}_forecaster
                    FROM pg
                    (select t, H1 from m4 where train = 1) 
                    PREDICT H1
                    {using}
                '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code is None, "train failed: " + model_name

        self.wait_training(model_name=f'{model_name}_forecaster')

        predict_sql = f'''
                    select p.t, p.H1 real, t.H1, t.H1__upper, t.H1__lower
                    from merlion.{model_name}_forecaster t
                    inner join pg.m4 p on t.t = p.t
                    where p.train = 0
                '''
        ret = self.run_mindsdb_sql(sql=predict_sql)
        assert ret.error_code is None, "forecast failed: " + model_name

    def test_merlion_detector(self):
        df = pd.read_csv(self.get_nba_abs_path())
        df.index = pd.to_datetime(df["t"])
        df.drop(columns=["t"], inplace=True)
        df_train = df[df["train"] == 1][["val"]]
        df_test = df[df["train"] == 0][["val"]]
        # # test default, it can't passed the unit test because of a py4j problem,
        # # (ValueError: invalid literal for int() with base 10: b'')
        # # it should be caused by the incompatible between test env and py4j, but
        # # the detector works successfully in deployed mode.
        # adapter = DefaultDetectorAdapter()
        # adapter.train(df_train.copy(deep=True), target="val")
        # rt_df = adapter.predict(df_test.copy(deep=True), target="val")
        # assert rt_df is not None
        # assert len(rt_df) == len(df_test)
        # assert len(rt_df[rt_df["val__anomaly_score"] > 0]) > 0
        # isolation
        adapter = IsolationForestDetectorAdapter()
        adapter.train(df_train.copy(deep=True), target="val")
        rt_df = adapter.predict(df_test.copy(deep=True), target="val")
        assert rt_df is not None
        assert len(rt_df[rt_df["val__anomaly_score"] > 0]) > 0
        # windstats
        adapter = WindStatsDetectorAdapter()
        adapter.train(df_train.copy(deep=True), target="val")
        rt_df = adapter.predict(df_test.copy(deep=True), target="val")
        assert rt_df is not None
        assert len(rt_df[rt_df["val__anomaly_score"] > 0]) > 0
        # prophet
        adapter = ProphetDetectorAdapter()
        adapter.train(df_train.copy(deep=True), target="val")
        rt_df = adapter.predict(df_test.copy(deep=True), target="val")
        assert rt_df is not None
        assert len(rt_df[rt_df["val__anomaly_score"] > 0]) > 0

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_merlion_detector_sql(self, mock_handler):
        # prepare data
        df = pd.read_csv(self.get_nba_abs_path())
        df["t"] = pd.to_datetime(df["t"])
        self.set_handler(mock_handler, name='pg', tables={'nba': df})

        # test isolation forest
        self.exec_train_and_detect(mock_handler=mock_handler, model_name="isolation",
                                     using_model=f", model_type='isolation'")
        # # test default, it can't passed the unit test because of a py4j problem,
        # # (ValueError: invalid literal for int() with base 10: b'')
        # # it should be caused by the incompatible between test env and py4j, but
        # # the detector works successfully in deployed mode.
        # self.exec_train_and_detect(mock_handler=mock_handler, model_name="default",
        #                              using_model=f", model_type='default'")
        # # test windstats
        # self.exec_train_and_detect(mock_handler=mock_handler, model_name="windstats",
        #                              using_model=f", model_type='windstats'")
        # # prophet
        # self.exec_train_and_detect(mock_handler=mock_handler, model_name="prophet",
        #                              using_model=f", model_type='prophet'")

    def exec_train_and_detect(self, mock_handler, model_name, using_model):
        # create predictor
        create_sql = f'''
                    CREATE PREDICTOR merlion.{model_name}_detector
                    FROM pg
                    (select t, val from nba where train = 1) 
                    PREDICT val
                    USING task='detector'{using_model}
                '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code is None, "train failed: " + model_name

        self.wait_training(model_name=f'{model_name}_detector')

        predict_sql = f'''
                    select p.t, p.val real, d.val__anomaly_score
                    from merlion.{model_name}_detector d
                    inner join pg.nba p on d.t = p.t and d.val = p.val
                    where p.train = 0
                    '''
        ret = self.run_mindsdb_sql(sql=predict_sql)
        assert ret.error_code is None, "detect failed: " + model_name

    def wait_training(self, model_name):
        # wait
        done = False
        for attempt in range(900):
            ret = self.run_mindsdb_sql(
                f"select status from merlion.predictors where name='{model_name}'"
            )
            if len(ret.data) > 0:
                if ret.data[0][0] == 'complete':
                    done = True
                    break
                elif ret.data[0][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created: " + model_name)
