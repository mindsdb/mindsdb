from mcp.types import Completion, PromptReference, ResourceTemplateReference

from mindsdb.api.mcp.mcp_instance import mcp
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.utilities.context import context as ctx
from mindsdb.api.mcp.resources.schema import _get_database_names
from mindsdb.utilities import log

logger = log.getLogger(__name__)


@mcp.completion()
async def handle_completion(ref, argument, context):
    if not isinstance(ref, (ResourceTemplateReference, PromptReference)):
        return None

    try:
        if argument.name == "database_name":
            names = _get_database_names()
            return Completion(values=[n for n in names if n.startswith(argument.value)])

        if argument.name == "table_name":
            database_name = (context.arguments or {}).get("database_name")
            if not database_name:
                return None
            ctx.set_default()
            session = SessionController()
            datanode = session.datahub.get(database_name)
            all_tables = datanode.get_tables()
            names = [t.TABLE_NAME for t in all_tables]
            return Completion(values=[n for n in names if n.startswith(argument.value)])
    except Exception as e:
        logger.info(f"Couldn't get completion for parameter {argument.name}: {e}")

    return None
