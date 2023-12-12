import os
import time

import pandas as pd
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

WRITER_API_KEY = os.environ.get("WRITER_API_KEY")
os.environ["WRITER_API_KEY"] = WRITER_API_KEY

WRITER_ORG_ID = os.environ.get("WRITER_ORG_ID")
os.environ["WRITER_ORG_ID"] = WRITER_ORG_ID


class TestRAG(BaseExecutorTest):
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

    def test_completion(self):
        # create project
        self.run_sql("create database proj")

        # # self.run_sql(
        # #     """
        # #     CREATE MODEL proj.test_litellm_handler_on_completion_openai
        # #     PREDICT text
        # #     USING
        # #     engine="litellm",
        # #     model="openai"
        # #     """
        # # )
        #
        # self.wait_predictor("proj", "test_litellm_handler_on_completion_openai")
