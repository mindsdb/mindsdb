from mindsdb.integrations.libs.const import HANDLER_TYPE, HANDLER_MAINTAINER

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example

try:
    from .snowflake_handler import SnowflakeHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e


title = "Snowflake"
name = "snowflake"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"
maintainer = HANDLER_MAINTAINER.MINDSDB

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "maintainer",
    "connection_args",
    "connection_args_example",
    "import_error",
    "icon_path",
]
