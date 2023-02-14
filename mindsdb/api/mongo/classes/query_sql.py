from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
# from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands
from mindsdb.api.mysql.mysql_proxy.executor import Executor
from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands

class SqlServerStub:
    """This class is just an emulation of Server object,
    used by Executor.
    In 'monolithic' mode of MindsDB work the Executor takes
    some information from the sql server which. Here we emulate
    this object."""

    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])


def run_sql_command(mindsdb_env, ast_query):
    sql_session = SessionController()
    sql_session.database = 'mindsdb'
    sqlserver = SqlServerStub(connection_id=-1)

    executor = Executor(sql_session, sqlserver)
    executor.query_execute(ast_query.to_string())
    if executor.error_code is not None:
        raise Exception(executor.error_message)

    if executor.columns is None:
        # return no data
        return []

    column_names = [
        c.name is c.alias is None or c.alias
        for c in executor.columns
    ]

    data = []
    for row in executor.data:
        data.append(dict(zip(column_names, row)))

    return data


# def run_sql_command(mindsdb_env, ast_query):
#     sql_session = SessionController()
#     sql_session.database = 'mindsdb'
# 
#     print(f"execute ast_query - {ast_query} of type {type(ast_query)}")
#     command_executor = ExecuteCommands(sql_session, executor=None)
#     ret = command_executor.execute_command(ast_query)
#     if ret.error_code is not None:
#         raise Exception(ret.error_message)
# 
#     if ret.columns is None:
#         # return no data
#         return []
# 
#     column_names = [
#         c.name is c.alias is None or c.alias
#         for c in ret.columns
#     ]
# 
#     data = []
#     for row in ret.data:
#         data.append(dict(zip(column_names, row)))
# 
#     return data
