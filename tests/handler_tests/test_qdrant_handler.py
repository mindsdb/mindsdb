import importlib
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("qdrant")
    QDRANT_INSTALLED = True
except ImportError:
    QDRANT_INSTALLED = False


@pytest.mark.skipif(not QDRANT_INSTALLED, reason="qdrant handler is not installed")
class TestQdrantHandler(BaseExecutorTest):
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
        # connect to a Qdrant instance and create a database
        tmp_directory = tempfile.mkdtemp()
        self.run_sql(
            f"""
            CREATE DATABASE qdrant_test
            WITH ENGINE = "qdrant",
            PARAMETERS = {{
                "location" : ":memory:"
            }}
        """
        )