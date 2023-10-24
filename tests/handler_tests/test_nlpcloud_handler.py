import importlib
import os
import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("nlpcloud")
    NLPCLOUD_INSTALLED = True
except ImportError:
    NLPCLOUD_INSTALLED = False


@pytest.mark.skipif(not NLPCLOUD_INSTALLED, reason="nlpcloud package is not installed")
class TestOilPriceAPIHandler(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        self.token = os.environ.get("NLPCLOUD_API_TOKEN")
        self.model = os.environ.get("NLPCLOUD_MODEL")
        self.run_sql(f"""
                     CREATE DATABASE mindsdb_nlpcloud
                        WITH ENGINE = 'nlpcloud',
                        PARAMETERS = {
                        "token": '{self.token}',
                        "model": '{self.model}'
                    };
        """)

    def test_basic_select_from(self):
        sql = 'SELECT * FROM mindsdb_nlpcloud.translation where text="hello mindsdb! How are ya?" AND source="eng_Latn" AND target="fra_Latn";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_nlpcloud.sentiment_analysis where text="Mindsdb is an excellent product";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_nlpcloud.language_detection where text="你好你好吗";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_nlpcloud.named_entity_recognition where text="The World Health Organization (WHO)[1] is a specialized agency of the United Nations responsible for international public health.";'
        self.run_sql(sql)
