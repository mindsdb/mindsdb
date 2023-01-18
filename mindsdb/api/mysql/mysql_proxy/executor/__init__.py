import os


executor_service_url = os.environ.get("MINDSDB_EXECUTOR_URL", None)

if executor_service_url is not None:
    from mindsdb.integrations.handlers_client.executor_client import ExecuteCommandsClient as ExecuteCommands
else:
    from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands

