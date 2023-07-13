import time
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