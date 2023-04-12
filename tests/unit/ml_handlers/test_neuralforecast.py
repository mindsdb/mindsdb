import time
import pytest
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

from tests.unit.ml_handlers.test_time_series_utils import create_mock_df
from tests.unit.executor_test_base import BaseExecutorTest


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
           where t.group_col_2='a2' AND t.time_col > LATEST
        """
        )
        assert list(round(result_df["target_col"])) == [32, 33, 34]

        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_multi_group as p
           where t.group_col='b' AND t.time_col > LATEST
        """
        )
        assert list(round(result_df["target_col"])) == [42, 43, 44]

        describe_result = self.run_sql("describe proj.model_multi_group")
        assert describe_result["inputs"][0] == ["target_col", "time_col", ["group_col", "group_col_2", "group_col_3"]]
        assert describe_result["outputs"][0] == "target_col"
        # The expected format of the "accuracies" key is
        # [(model_1_name, model_1_accuracy), (model_2_name, model_2_accuracy), ...]
        assert describe_result["accuracies"][0][0][0] == "NHITS"
        assert describe_result["accuracies"][0][0][1] < 1

        describe_model = self.run_sql("describe proj.model_multi_group.model")
        assert describe_model["model_name"][0] == "NHITS"
        assert describe_model["frequency"][0] == "Q"

        describe_features = self.run_sql("describe proj.model_multi_group.features")
        assert describe_features["ds"][0] == "time_col"
        assert describe_features["y"][0] == "target_col"
        assert describe_features["unique_id"][0] == ["group_col", "group_col_2", "group_col_3"]

        with pytest.raises(Exception) as e:
            self.run_sql("describe proj.modelx.ensemble")
            assert "ensemble is not supported" in str(e)

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
        self.wait_predictor("proj", "model_exog_var")

        result_df = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_exog_var as p
           where t.group_col='b' AND t.time_col > LATEST
        """
        )
        assert list(round(result_df["target_col"])) == [42, 43, 44]

        describe_result = self.run_sql("describe proj.model_exog_var")
        assert describe_result["inputs"][0] == ["target_col", "time_col", ["group_col", "group_col_2", "group_col_3"], "exog_var_1"]

        describe_features = self.run_sql("describe proj.model_exog_var.features")
        assert describe_features["exog_vars"][0] == ["exog_var_1"]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_hierarchical(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/house_sales.csv")  # comes mindsdb docs forecast example
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
           create model proj.model_1_group
           from pg (select * from df)
           predict ma
           order by saledate
           group by type, bedrooms
           horizon 4
           window 8
           using
             engine='neuralforecast',
             hierarchy=['type', 'bedrooms'],
             train_time=0.01
        """
        )
        self.wait_predictor("proj", "model_1_group")

        # run predict
        mindsdb_result_hier = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.model_1_group as p
           where t.type='house'
        """
        )
        assert len(list(round(mindsdb_result_hier["ma"])))

        describe_result = self.run_sql("describe proj.model_1_group.model")
        assert describe_result["hierarchy"][0] == ["type", "bedrooms"]
