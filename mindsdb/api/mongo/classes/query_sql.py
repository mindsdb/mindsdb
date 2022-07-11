from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController

from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands

def run_sql_command(mindsdb_env, ast_query):
    # empty object
    server_obj = type('', (), {})()

    server_obj.original_integration_controller = mindsdb_env['origin_integration_controller']
    server_obj.original_model_interface = mindsdb_env['origin_model_interface']
    server_obj.original_view_controller = mindsdb_env['origin_view_controller']

    sql_session = SessionController(
        server=server_obj,
        company_id=mindsdb_env['company_id']
    )
    sql_session.database = 'mindsdb'

    command_executor = ExecuteCommands(sql_session, executor=None)
    ret = command_executor.execute_command(ast_query)
    if ret.error_code is not None:
        raise Exception(ret.error_message)

    column_names = [
        c.name is c.alias is None or c.alias
        for c in ret.columns
    ]

    data = []
    for row in ret.data:
        data.append(dict(zip(column_names, row)))

    return data

