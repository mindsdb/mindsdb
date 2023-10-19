from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


class TestSpacy(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_recognition(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_spacy_ner
            PREDICT recognition
            USING
              engine = 'spacy',
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.test_spacy_ner
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion'
            """
        )

        assert "{(28, 32, 'GPE'), (45, 55, 'MONEY'), (1, 6, 'ORG')}" in result_df["recognition"].iloc[0]
