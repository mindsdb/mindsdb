from mindsdb.integrations.libs.const import HANDLER_SUPPORT_LEVEL, HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version
from .connection_args import connection_args, connection_args_example

try:
    from .moss_handler import MossHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Moss"
name = "moss"
type = HANDLER_TYPE.DATA
support_level = HANDLER_SUPPORT_LEVEL.COMMUNITY
icon_path = "icon.png"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "support_level",
    "connection_args",
    "connection_args_example",
    "import_error",
    "icon_path",
]
