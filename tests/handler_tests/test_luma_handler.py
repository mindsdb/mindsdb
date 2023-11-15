import importlib
import os
import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("requests")
    REQUESTS_INSTALLED = True
except ImportError:
    REQUESTS_INSTALLED = False


@pytest.mark.skipif(not REQUESTS_INSTALLED, reason="requests package is not installed")
class TestLumaHandler(BaseExecutorTest):

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
        self.api_key = os.environ.get("LUMA_API_KEY")
        self.run_sql(f"""
            CREATE DATABASE mindsdb_luma
                WITH ENGINE = 'luma',
                PARAMETERS = {
                "api_key": '{self.api_key}'
                };
        """)

    def test_basic_select_from(self):
        sql = "SELECT * FROM mindsdb_luma.events;"
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_luma.events where event_id = "evt-HQ36IFDwncocuGy";'
        assert self.run_sql(sql).shape[0] == 1
