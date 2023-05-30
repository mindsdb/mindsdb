from unittest.mock import patch

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorTest
import time

# move to conftest.py
QUESTION_DF = pd.DataFrame(
    {
        'question': ['what is the treatment for a cough?'],
        'context': ['cough sirup is the most effective treatment for a cough.'],
    }
)

class TestWriter(BaseExecutorTest):
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
    def test_qa(self, mock_handler):
        # create project
        self.run_sql("create database proj")

        self.set_handler(mock_handler, name="pg", tables={"df": QUESTION_DF})

        # now add more groups
        self.run_sql(
            """
           create model proj.qa
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
