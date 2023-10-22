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
        self.api_key = os.environ.get("OILPRICEAPI_KEY")
        self.run_sql(f"""
            CREATE DATABASE mindsdb_oilpriceapi
                WITH ENGINE = 'oilpriceapi',
                PARAMETERS = {
                "api_key": '{self.api_key}'
                };
        """)

    def test_basic_select_from(self):
        sql = "SELECT * FROM mindsdb_oilpriceapi.latest_price;"
        assert self.run_sql(sql).shape[0] == 1

        sql = "SELECT * FROM mindsdb_oilpriceapi.past_day_price;"
        assert self.run_sql(sql).shape[0] == 20

    def test_complex_select(self):
        sql = 'SELECT price FROM mindsdb_oilpriceapi.latest_price where by_type="daily_average_price" and by_code="WTI_USD";'
        assert self.run_sql(sql).shape[1] == 1

        sql = "SELECT * FROM npm_test.past_day_price LIMIT 1;"
        assert self.run_sql(sql).shape[0] == 1
