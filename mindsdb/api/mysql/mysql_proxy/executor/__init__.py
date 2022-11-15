import os
from .executor import Executor
from .executor_commands import ExecuteCommands
from .data_types import *
from .executor_client import ExecutorClient


if os.environ.get("MINDSDB_EXECUTOR_SERVICE"):
    Executor = ExecutorClient
