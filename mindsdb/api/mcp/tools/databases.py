from mindsdb.api.mcp.mcp_instance import mcp
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.utilities.context import context as ctx


list_databases_tool_description = (
    "Returns a list of all database connections currently available in MindsDB. "
    + "The tool takes no parameters and responds with a list of database names, "
    + 'for example: ["my_postgres", "my_mysql", "test_db"].'
)


@mcp.tool(name="list_databases", description=list_databases_tool_description)
def list_databases() -> list[str]:
    ctx.set_default()
    session = SessionController()
    databases = session.database_controller.get_list()
    return [x['name'] for x in databases if x['type'] == 'data']
