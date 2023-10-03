from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .orientdb_handler import OrientDBHandler as Handler
    from .orientdb_handler import connection_args, connection_args_example
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __description__ as description
from .__about__ import __version__ as version

title = "OrientDB"
name = "orientdb"
type = HANDLER_TYPE.DATA
icon_path = "icon.png"

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
