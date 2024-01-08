
import time

import pandas as pd
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


class TestSentenceTransformers(BaseExecutorTest):

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

    def test_sentence_transformers(self):
        """
        test that embeddings are created correctly
        """

        df = pd.DataFrame(data={"content": ["hello", "world", "how are you"]})

        self.save_file("df", df)

        # create project
        self.run_sql("create database proj")

        # create model

        self.run_sql(
            """
            create model proj.test_hf_embeddings
            predict content
            using engine="sentence_transformers"
            """
        )

        self.wait_predictor("proj", "test_hf_embeddings")

        # get embeddings from df

        ret = self.run_sql(
            """
            select * from proj.test_hf_embeddings
            where content = (select content from files.df)
            """
        )

        assert ret.shape == (3, 3)
