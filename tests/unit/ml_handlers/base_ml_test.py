import os
import time
import pandas as pd

from mindsdb_sql import parse_sql
from ..executor_test_base import BaseExecutorTest


class BaseMLTest(BaseExecutorTest):
    """
    Base test class for ML engines
    """
    def wait_predictor(self, project: str, name: str, timeout: int = 100) -> None:
        """Wait for the predictor to be created, raising an exception if predictor creation fails or exceeds timeout"""
        for attempt in range(timeout):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                status = ret["STATUS"][0]
                if status == "complete":
                    return
                elif status == "error":
                    raise RuntimeError("Predictor failed", ret["ERROR"][0])
            time.sleep(0.5)
        raise RuntimeError("Predictor wasn't created")

    def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return a DataFrame, raising an AssertionError if an error occurs"""
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None, f"SQL execution failed with error: {ret.error_code}"
        if ret.data is not None:
            columns = [col.alias if col.alias else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)


class BaseMLAPITest(BaseMLTest):
    """
    Base test class for API-based ML engines
    """
    @staticmethod
    def get_api_key(env_var: str):
        """Retrieve API key from environment variables"""
        return os.environ.get(env_var)
