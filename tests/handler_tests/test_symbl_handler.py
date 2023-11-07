import importlib
import os
import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("symbl")
    SYMBL_INSTALLED = True
except ImportError:
    SYMBL_INSTALLED = False


@pytest.mark.skipif(not SYMBL_INSTALLED, reason="symbl package is not installed")
class TestSymblAPIHandler(BaseExecutorTest):

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
        self.app_id = os.environ.get("SYMBL_APP_ID")
        self.app_secret = os.environ.get("SYMBL_APP_SECRET")
        self.run_sql(f"""
            CREATE DATABASE mindsdb_symbl
                    WITH ENGINE = 'symbl',
                    PARAMETERS = {
                    "app_id": '{self.app_id}',
                    "app_secret": '{self.app_secret}'
                    };
        """)

    def test_basic_select_from(self):

        conversation_id = "5682305049034752"

        sql = f'SELECT * FROM mindsdb_symbl.get_messages where conversation_id="{conversation_id}"'
        self.run_sql(sql)

        sql = f'SELECT * FROM mindsdb_symbl.get_topics where conversation_id="{conversation_id}"'
        self.run_sql(sql)

        sql = f'SELECT * FROM mindsdb_symbl.get_questions where conversation_id="{conversation_id}"'
        self.run_sql(sql)

        sql = f'SELECT * FROM mindsdb_symbl.get_analytics where conversation_id="{conversation_id}"'
        self.run_sql(sql)

        sql = f'SELECT * FROM mindsdb_symbl.get_action_items where conversation_id="{conversation_id}"'
        self.run_sql(sql)

        sql = f'SELECT * FROM mindsdb_symbl.get_follow_ups where conversation_id="{conversation_id}"'
        self.run_sql(sql)
