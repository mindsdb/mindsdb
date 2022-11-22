from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands


def run_sql_command(mindsdb_env, ast_query):
    sql_session = SessionController()
    sql_session.database = 'mindsdb'

    command_executor = ExecuteCommands(sql_session, executor=None)
    ret = command_executor.execute_command(ast_query)
    if ret.error_code is not None:
        raise Exception(ret.error_message)

    if ret.columns is None:
        # return no data
        return []

    column_names = [
        c.name is c.alias is None or c.alias
        for c in ret.columns
    ]

    data = []
    for row in ret.data:
        data.append(dict(zip(column_names, row)))

    return data
