import pytest

from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_client import ExecutorClient
from mindsdb.integrations.handlers_client.db_client import DBServiceClient
from mindsdb.integrations.handlers_client.ml_client import MLClient
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)


def get_call_args(f_obj):
    code = f_obj.__code__
    return code.co_varnames[: code.co_argcount]


@pytest.mark.parametrize(
    "origin,compared",
    [
        (Executor, ExecutorClient),
        (ExecutorClient, Executor),
        (PostgresHandler, DBServiceClient.client_class),
        (DBServiceClient.client_class, PostgresHandler),
        (BaseMLEngineExec, MLClient.client_class),
    ],
)
def test_equal_public_api(origin, compared):
    for attr_name, attr in origin.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                assert (
                    attr_name in compared.__dict__
                ), f"'{attr_name}' must be a part of {compared} public API"
                assert callable(
                    compared.__dict__[attr_name]
                ), f"'{attr_name}' must be a method"


@pytest.mark.parametrize(
    "origin,compared",
    [
        (Executor, ExecutorClient),
        (PostgresHandler, DBServiceClient.client_class),
    ],
)
def test_equal_annotation(origin, compared):
    for attr_name, attr in origin.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                annotation_in_exec = attr.__annotations__
                annotation_in_client = compared.__dict__[attr_name].__annotations__
                assert (
                    annotation_in_client == annotation_in_exec
                ), f"'{attr_name}' method must have equal annotaion in {origin} and {compared}"
