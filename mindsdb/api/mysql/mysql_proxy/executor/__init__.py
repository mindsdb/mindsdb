import os
# from .executor import Executor
from .executor_commands import ExecuteCommands
from .data_types import *
# from .executor_client import ExecutorClient
from .executor_client_factory import ExecutorClient


# In case of Executor Service need to use ExecutorClient instead of Executor
# so here we hide the difference between two objects
# if os.environ.get("MINDSDB_EXECUTOR_URL"):
Executor = ExecutorClient
