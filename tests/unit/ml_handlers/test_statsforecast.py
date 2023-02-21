import time
from unittest.mock import patch
import numpy as np
import pandas as pd

from mindsdb.integrations.handlers.statsforecast_handler.statsforecast_handler import StatsForecastHandler, transform_to_nixtla_df
from statsforecast.models import AutoARIMA
from statsforecast import StatsForecast
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


def test_statsforecast_df_transformations():
    sf_handler = StatsForecastHandler("model_storage", "engine_storage")
    df = create_mock_df()
    settings_dict = {"order_by": "time_col", "group_by": ["group_col"], "target": "target_col"}

    # Test transform for single groupby
    sf_df = transform_to_nixtla_df(df, settings_dict)
    assert [sf_df["unique_id"].iloc[i] == df["group_col"].iloc[i] for i in range(len(sf_df))]
    assert [sf_df["y"].iloc[i] == df["target_col"].iloc[i] for i in range(len(sf_df))]
    assert [sf_df["ds"].iloc[i] == df["time_col"].iloc[i] for i in range(len(sf_df))]
    # Test reversing the transform
    sf_results_df = sf_df.rename({"y": "AutoARIMA"}, axis=1).set_index("unique_id")
    mindsdb_results_df = sf_handler._get_results_from_statsforecast_df(sf_results_df, settings_dict)
    pd.testing.assert_frame_equal(mindsdb_results_df, df[["time_col", "target_col", "group_col"]])

    # Test for multiple groups
    settings_dict["group_by"] = ["group_col", "group_col_2", "group_col_3"]
    sf_df = transform_to_nixtla_df(df, settings_dict)
    assert sf_df["unique_id"][0] == "a|a2|a3"
    # Test reversing the transform
    sf_results_df = sf_df.rename({"y": "AutoARIMA"}, axis=1).set_index("unique_id")
    mindsdb_results_df = sf_handler._get_results_from_statsforecast_df(sf_results_df, settings_dict)
    pd.testing.assert_frame_equal(mindsdb_results_df, df)


class TestStatsForecast(BaseExecutorTest):
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

        self.run_sql(
            """
           create model proj.model_1_group
           from pg (select * from df)
           predict target_col
           order by time_col
           group by group_col
           horizon 3
           using
             engine='statsforecast'
        """
        )
        self.wait_predictor("proj", "model_1_group")

        # run predict
        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_1_group as p
           where t.group_col='b'
        """
        )
        assert list(round(result_df["target_col"])) == [42, 43, 44]

        # now add more groups
        self.run_sql(
            """
           create model proj.model_multi_group
           from pg (select * from df)
           predict target_col
           order by time_col
           group by group_col, group_col_2, group_col_3
           horizon 3
           using
             engine='statsforecast'
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

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_ts_series(self, mock_handler):
        """This sends a dataframe where the data is already in time series format i.e.
        doesn't need grouped
        """

        # create project
        self.run_sql("create database proj")

        # mock a time series dataset
        df = pd.read_parquet('https://datasets-nixtla.s3.amazonaws.com/m4-hourly.parquet')
        df = df[df.unique_id.isin(["H1", "H2", "H3"])]  # subset for speed
        n_groups = df["unique_id"].nunique()
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # generate ground truth predictions from the package
        prediction_horizon = 4
        sf = StatsForecast(models=[AutoARIMA()], freq="Q")
        sf.fit(df)
        forecast_df = sf.forecast(prediction_horizon)
        package_predictions = forecast_df.reset_index(drop=True).iloc[:, -1]

        # create predictor
        self.run_sql(
            f"""
           create model proj.modelx
           from pg (select * from df)
           predict y
           order by ds
           group by unique_id
           horizon {prediction_horizon}
           using
             engine='statsforecast'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )

        # check against ground truth
        mindsdb_result = result_df.iloc[:, -1]
        assert len(mindsdb_result) == prediction_horizon * n_groups
        assert np.allclose(mindsdb_result, package_predictions)
