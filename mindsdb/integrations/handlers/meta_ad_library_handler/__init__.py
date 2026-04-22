from mindsdb.integrations.libs.const import HANDLER_SUPPORT_LEVEL, HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .connection_args import connection_args, connection_args_example
    from .meta_ad_library_handler import MetaAdLibraryHandler as Handler

    import_error = None
except Exception as exc:
    Handler = None
    connection_args = None
    connection_args_example = None
    import_error = exc


title = "Meta Ad Library"
name = "meta_ad_library"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"
support_level = HANDLER_SUPPORT_LEVEL.COMMUNITY

__all__ = [
    "Handler",
    "connection_args",
    "connection_args_example",
    "description",
    "icon_path",
    "import_error",
    "name",
    "support_level",
    "title",
    "type",
    "version",
]
