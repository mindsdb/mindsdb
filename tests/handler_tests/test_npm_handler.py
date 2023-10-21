import importlib

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
class TestNPMHandler(BaseExecutorTest):

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
        self.test_package_name = "handlebars"
        self.run_sql("""
            CREATE DATABASE npm_test
            WITH ENGINE = "npm";
        """)

    def test_basic_select_from(self):
        # Select from metadata table
        sql = f"""
            SELECT * FROM npm_test.metadata WHERE package='{self.test_package_name}';
        """
        assert self.run_sql(sql).shape[0] == 1

        # Select from maintainers table
        sql = f"""
            SELECT * FROM npm_test.maintainers WHERE package='{self.test_package_name}';
        """
        self.run_sql(sql)

        # Select from keywords table
        sql = f"""
            SELECT * FROM npm_test.keywords WHERE package='{self.test_package_name}';
        """
        self.run_sql(sql)

        # Select from dependencies table
        sql = f"""
            SELECT * FROM npm_test.dependencies WHERE package='{self.test_package_name}';
        """
        self.run_sql(sql)

        # Select from dev_dependencies table
        sql = f"""
            SELECT * FROM npm_test.dev_dependencies WHERE package='{self.test_package_name}';
        """
        self.run_sql(sql)

        # Select from optional_dependencies table
        sql = f"""
            SELECT * FROM npm_test.optional_dependencies WHERE package='{self.test_package_name}';
        """
        self.run_sql(sql)

        # Select from github_stats table
        sql = f"""
            SELECT * FROM npm_test.github_stats WHERE package='{self.test_package_name}';
        """
        assert self.run_sql(sql).shape[0] == 1

    def test_complex_select(self):
        # Select email maintainers table
        sql = f"""
            SELECT email FROM npm_test.maintainers WHERE package='{self.test_package_name}';
        """
        assert self.run_sql(sql).shape[1] == 1

        # Select single dependency
        sql = f"""
            SELECT * FROM npm_test.dependencies WHERE package='{self.test_package_name}' LIMIT 1;
        """
        assert self.run_sql(sql).shape[0] == 1
