from mindsdb.api.mysql.mysql_proxy.controllers import SessionController
from mindsdb.api.mysql.mysql_proxy.executor import Executor


class SqlServerStub:
    """This class is just an emulation of Server object,
    used by Executor.
    In 'monolithic' mode of MindsDB work the Executor takes
    some information from the sql server which. Here we emulate
    this object."""

    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])


def run_sql_command(request_env, ast_query):
    sql_session = SessionController()
    sql_session.database = request_env.get('database', 'mindsdb')
    sqlserver = SqlServerStub(connection_id=-1)

    executor = Executor(sql_session, sqlserver)
    executor.binary_query_execute(ast_query)
    if executor.error_code:
        raise Exception(executor.error_message)

    if not executor.columns:
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
