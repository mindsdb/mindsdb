import os
import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

# How to run:
#   env PYTHONPATH=./ pytest tests/unit/test_merlion_handler.py
from mindsdb.integrations.handlers.merlion_handler.adapters import DefaultForecasterAdapter, SarimaForecasterAdapter, \
    ProphetForecasterAdapter, MSESForecasterAdapter, IsolationForestDetectorAdapter, \
    WindStatsDetectorAdapter, ProphetDetectorAdapter
from .executor_test_base import BaseExecutorTest

import pandas as pd


class TestMerlion(BaseExecutorTest):
    def set_project(self):
        r = self.db.Project.query.filter_by(name='mindsdb').first()
        if r is not None:
            self.db.session.delete(r)

        r = self.db.Project(
            id=1,
            name='mindsdb',
        )
        self.db.session.add(r)
        self.db.session.commit()

    def get_m4_df(self) -> pd.DataFrame:
        train_csv = "https://raw.githubusercontent.com/Mcompetitions/M4-methods/master/Dataset/Train/Hourly-train.csv"
        test_csv = "https://raw.githubusercontent.com/Mcompetitions/M4-methods/master/Dataset/Test/Hourly-test.csv"
        train_set = pd.read_csv(train_csv).set_index("V1")
        test_set = pd.read_csv(test_csv).set_index("V1")
        ntrain = train_set.iloc[0, :].dropna().shape[0]
        sequence = pd.concat((train_set.iloc[0, :].dropna(), test_set.iloc[0, :].dropna()))
        # raw data do not follow consistent timestamp format
        sequence.index = pd.date_range(start=0, periods=sequence.shape[0], freq="H")
        sequence = sequence.to_frame()
        metadata = pd.DataFrame({"trainval": sequence.index < sequence.index[ntrain]}, index=sequence.index)
        train = sequence[metadata.trainval]
        test = sequence[~metadata.trainval]
        train["train"] = 1
        test["train"] = 0
        df = pd.concat([train, test], axis=0)
        return df

    def get_nab_df(self) -> pd.DataFrame:
        df = pd.read_csv("https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/nyc_taxi.csv")
        df.rename(columns={"timestamp": "t", "value": "val"}, inplace=True)
        train_len = int(len(df) * 0.5)
        df_train = df.iloc[: train_len]
        df_test = df.iloc[train_len: ]
        df_train["train"] = 1
        df_test["train"] = 0
        df = pd.concat([df_train, df_test], axis=0)
        return df

    def run_mindsdb_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )

    def test_merlion_forecaster(self):
        df = self.get_m4_df()
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
        self.set_project()
        # prepare data
        df = self.get_m4_df()
        df["t"] = df.index
        self.set_handler(mock_handler, name='pg', tables={'m4': df})
        # test default
        self.exec_train_and_forecast(mock_handler=mock_handler, model_name="default", using="")

    def exec_train_and_forecast(self, mock_handler, model_name, using):
        # create predictor
        create_sql = f'''
                    CREATE PREDICTOR mindsdb.{model_name}_forecaster
                    FROM pg
                    (select t, H1 from m4 where train = 1) 
                    PREDICT H1
                    USING engine='merlion'{using}
                '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code is None, "train failed: " + model_name

        self.wait_training(model_name=f'{model_name}_forecaster')

        predict_sql = f'''
                    select p.t, p.H1 real, t.H1, t.H1__upper, t.H1__lower
                    from mindsdb.{model_name}_forecaster t
                    inner join pg.m4 p on t.t = p.t
                    where p.train = 0
                '''
        ret = self.run_mindsdb_sql(sql=predict_sql)
        assert ret.error_code is None, "forecast failed: " + model_name

    def test_merlion_detector(self):
        df = self.get_nab_df()
        df.index = pd.to_datetime(df["t"])
        df.drop(columns=["t"], inplace=True)
        df_train = df[df["train"] == 1][["val"]]
        df_test = df[df["train"] == 0][["val"]]
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
        self.set_project()
        # prepare data
        df = self.get_nab_df()
        df["t"] = pd.to_datetime(df["t"])
        self.set_handler(mock_handler, name='pg', tables={'nba': df})

        # test isolation forest
        self.exec_train_and_detect(mock_handler=mock_handler, model_name="isolation",
                                     using_model=f", model_type='isolation'")

    def exec_train_and_detect(self, mock_handler, model_name, using_model):
        # create predictor
        create_sql = f'''
                    CREATE PREDICTOR mindsdb.{model_name}_detector
                    FROM pg
                    (select t, val from nba where train = 1) 
                    PREDICT val
                    USING engine='merlion', task='detector'{using_model}
                '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code is None, "train failed: " + model_name

        self.wait_training(model_name=f'{model_name}_detector')

        predict_sql = f'''
                    select p.t, p.val real, d.val__anomaly_score
                    from mindsdb.{model_name}_detector d
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
                f"select status from mindsdb.predictors where name='{model_name}'"
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
