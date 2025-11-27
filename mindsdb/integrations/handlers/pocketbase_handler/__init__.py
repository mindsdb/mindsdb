from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description, __version__ as version
from .connection_args import connection_args, connection_args_example

try:
    from .pocketbase_handler import PocketbaseHandler as Handler
    import_error = None
except Exception as exc:  # pragma: no cover - surfaced in UI
    Handler = None
    import_error = exc

title = "PocketBase"
name = "pocketbase"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "connection_args",
    "connection_args_example",
    "import_error",
    "icon_path",
]
