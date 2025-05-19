import os

from ..executor_test_base import BaseExecutorTest


class BaseMLAPITest(BaseExecutorTest):
    """
    Base test class for API-based ML engines
    """
    @staticmethod
    def get_api_key(env_var: str):
        """Retrieve API key from environment variables"""
        return os.environ.get(env_var)
