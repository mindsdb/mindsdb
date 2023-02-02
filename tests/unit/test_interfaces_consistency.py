import pytest

from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_client import ExecutorClient
from mindsdb.api.mysql.mysql_proxy.executor.executor_service import ExecutorService

from mindsdb.integrations.handlers_client.db_client import DBServiceClient
from mindsdb.integrations.handlers_wrapper.db_handler_wrapper import DBHandlerWrapper
from mindsdb.integrations.handlers_client.ml_client import MLClient
from mindsdb.integrations.handlers_wrapper.ml_handler_wrapper import MLHandlerWrapper
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)


def get_call_args(f_obj):
    code = f_obj.__code__
    return code.co_varnames[: code.co_argcount]


def get_public_api(obj):
    res = {}
    for attr_name, attr in obj.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                res[attr_name] = attr
    return res


@pytest.mark.parametrize(
    "origin,compared",
    [
        (Executor, ExecutorClient),
        (ExecutorClient, ExecutorService),
        (PostgresHandler, DBServiceClient.client_class),
        (DBServiceClient.client_class, DBHandlerWrapper),
        (BaseMLEngineExec, MLClient.client_class),
        (MLClient.client_class, MLHandlerWrapper),
    ],
)
def test_equal_public_api(origin, compared):
    origin_api = get_public_api(origin)
    compared_api = get_public_api(compared)
    assert len(origin_api.keys()) == len(
        compared_api.keys()
    ), f"number of methods in public API must be the same for {origin} and {compared}"
    for attr_name in origin_api:
        assert (
            attr_name in compared_api
        ), f"{attr_name} must be a part of {compared} public API"


@pytest.mark.parametrize(
    "origin,compared",
    [
        (Executor, ExecutorClient),
        (PostgresHandler, DBServiceClient.client_class),
        (BaseMLEngineExec, MLClient.client_class),
    ],
)
def test_equal_annotation(origin, compared):
    origin_api = get_public_api(origin)
    compared_api = get_public_api(compared)
    for attr_name, attr in origin_api.items():
        origin_annotation = attr.__annotations__
        compared_annotation = compared_api[attr_name].__annotations__
        assert (
            origin_annotation == compared_annotation
        ), f"'{attr_name}' method must have equal annotaion in {origin} and {compared}"
