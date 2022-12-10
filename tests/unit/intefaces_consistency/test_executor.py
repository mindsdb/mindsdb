import pytest

from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_client import ExecutorClient
from mindsdb.integrations.handlers_client.db_client import DBServiceClient
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler


def get_call_args(f_obj):
    code = f_obj.__code__
    return code.co_varnames[:code.co_argcount]

@pytest.mark.parametrize("origin,compared",
        [
            (Executor, ExecutorClient),
            (ExecutorClient, Executor),
            (PostgresHandler, DBServiceClient.client_class),
        ]
    )
def test_equal_public_api(origin, compared):
    for attr_name, attr in origin.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                assert attr_name in compared.__dict__, f"{attr_name} must be a part of {compared} public API"
                assert callable(compared.__dict__[attr_name]), f"{attr_name} must be a method" 


@pytest.mark.parametrize("origin,compared", [(Executor, ExecutorClient),])
def test_equal_annotation(origin, compared):
    for attr_name, attr in origin.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                annotation_in_exec = attr.__annotations__
                annotation_in_client = compared.__dict__[attr_name].__annotations__
                assert annotation_in_client == annotation_in_exec, f"{attr_name} must have equal annotaion in {origin} and {compared}"


@pytest.mark.parametrize("origin,compared", [(Executor, ExecutorClient),])
def test_methods_have_same_parameters(origin, compared):
    for attr_name, attr in origin.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                o_args = get_call_args(attr)
                c_args = get_call_args(compared.__dict__[attr_name])
                assert o_args == c_args, f"method '{attr_name}' must have same set of params in {origin}({o_args}) and {compared}({c_args})"
