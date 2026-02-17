from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example

try:
    from .netsuite_handler import NetSuiteHandler as Handler

    import_error = None
except Exception as e:  # pragma: no cover - surfaced to UI
    Handler = None
    import_error = e

title = "Oracle NetSuite"
name = "netsuite"
type = HANDLER_TYPE.DATA
icon_path = "netsuite.svg"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
    "connection_args",
    "connection_args_example",
]
