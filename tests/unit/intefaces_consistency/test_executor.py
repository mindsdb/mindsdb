
from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_client import ExecutorClient


def get_call_args(f_obj):
    code = f_obj.__code__
    return code.co_varnames[:code.co_argcount]

def test_executor_public_api_in_client():
    for attr_name, attr in Executor.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                assert attr_name in ExecutorClient.__dict__, f"{attr_name} must be a part of ExeclutorClients public API"
                assert callable(ExecutorClient.__dict__[attr_name]), f"{attr_name} must be a method" 

def test_client_public_api_in_executor():
    for attr_name, attr in ExecutorClient.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                assert attr_name in Executor.__dict__, f"{attr_name} must be a part of ExeclutorClients public API"
                assert callable(Executor.__dict__[attr_name]), f"{attr_name} must be a method" 


def test_equal_annotation_client_executor():
    for attr_name, attr in Executor.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                annotation_in_exec = attr.__annotations__
                annotation_in_client = ExecutorClient.__dict__[attr_name].__annotations__
                assert annotation_in_client == annotation_in_exec, f"{attr_name} must have equal annotaion in Executor and ClientExecutor"


def test_methods_have_same_parameters():
    for attr_name, attr in Executor.__dict__.items():
        if not attr_name.startswith("_"):
            if callable(attr):
                e_args = get_call_args(attr)
                c_args = get_call_args(ExecutorClient.__dict__[attr_name])
                assert e_args == c_args, f"method '{attr_name}' must have same set of params in Executor({e_args}) and Client({c_args})"
