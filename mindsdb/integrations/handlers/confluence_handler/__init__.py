from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities.log import get_log

logger = get_log()

from .__about__ import __version__ as version, __description__ as description

try:
    from .confluence_handler import ConfluenceHandler as Handler
    logger.error("No error Importing Confluence API")
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
    logger.error(f"Error Importing Confluence API: {e}!")

title = "Confluence"
name = "confluence"
type = HANDLER_TYPE.DATA
icon_path = "icon.png"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
]
