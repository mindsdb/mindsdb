import time

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..executor_test_base import BaseExecutorTest


class TestPytorchTabular(BaseExecutorTest):
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
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def test_invalid_task(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.invalid_task
            FROM files (
            SELECT FROM California_Housing
            )
            PREDICT median_house_value
            USING
                engine='pytorch_tabular',
                target = 'medain_house_value',
                initialization = 'xavier',
                task='invalid_task',
                continuous_cols=["longitude", "latitude", "housing_median_age", "total_rooms", "total_bedrooms", "population", "households", "median income"],
                categorical_cols = ["ocean_proximity"],
                epochs = 5,
                batch_size = 32
            """
        )
        with pytest.raises(RuntimeError):
            self.wait_predictor("proj", "invalid_task")

    def test_invalid_initialization(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.invalid_initialization
            FROM files (
            SELECT FROM California_Housing
            )
            PREDICT median_house_value
            USING
                engine='pytorch_tabular',
                target = 'medain_house_value',
                initialization = 'invalid_initialization',
                task='regression',
                continuous_cols=["longitude", "latitude", "housing_median_age", "total_rooms", "total_bedrooms", "population", "households", "median income"],
                categorical_cols = ["ocean_proximity"],
                epochs = 5,
                batch_size = 32
            """
        )
        with pytest.raises(RuntimeError):
            self.wait_predictor("proj", "invalid_initialization")

    def test_invalid_layers(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            CREATE MODEL proj.invalid_layers
            FROM files (
            SELECT FROM California_Housing
            )
            PREDICT median_house_value
            USING
                engine='pytorch_tabular',
                target = 'medain_house_value',
                initialization = 'xavier',
                task='regression',
                layers = 'invalid_layers',
                continuous_cols=["longitude", "latitude", "housing_median_age", "total_rooms", "total_bedrooms", "population", "households", "median income"],
                categorical_cols = ["ocean_proximity"],
                epochs = 5,
                batch_size = 32
            """
        )
        with pytest.raises(RuntimeError):
            self.wait_predictor("proj", "invalid_layers")
