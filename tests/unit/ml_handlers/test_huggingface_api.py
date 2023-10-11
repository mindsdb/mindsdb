from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


class TestHuggingFaceAPI(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_text_classification(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        texts = ["I like you. I love you", "I don't like you. I hate you"]
        df = pd.DataFrame(texts, columns=['texts'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_hfapi_text_classification
            PREDICT sentiment
            USING
              task = 'text-classification',
              engine = 'hf_api_engine',
              api_key = '<YOUR_API_KEY>',
              input_column = 'text'
            """
        )

        result_df = self.run_sql(
            """
            SELECT sentiment
            FROM proj.test_hfapi_text_classification
            WHERE
            text='I like you. I love you'
            """
        )

        assert "positive" in result_df["sentiment"].iloc[0].lower()
