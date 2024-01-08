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
class TestZipCodeBaseHandler(BaseExecutorTest):

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
        self.api_key = os.environ.get("ZIPCODEBASE_API_KEY")
        self.run_sql(f"""
            CREATE DATABASE mindsdb_zipcodebase
                WITH ENGINE = 'zipcodebase',
                PARAMETERS = {
                "api_key": '{self.api_key}'
                };
        """)

    def test_basic_select_from(self):
        sql = "SELECT * FROM mindsdb_zipcodebase.code_to_location where codes='10005';"
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_zipcodebase.codes_within_radius WHERE code="10005" AND radius="100" AND country="us";'
        self.run_sql(sql)

    def test_complex_select(self):
        sql = 'SELECT state FROM mindsdb_zipcodebase.codes_within_radius WHERE code="10005" AND radius="100" AND country="us";'
        assert self.run_sql(sql).shape[1] == 6

        sql = "SELECT * FROM mindsdb_zipcodebase.code_to_location where codes='10005'; LIMIT 1;"
        assert self.run_sql(sql).shape[0] == 1
