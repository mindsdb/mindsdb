from mindsdb.integrations.libs.const import HANDLER_TYPE, HANDLER_SUPPORT_LEVEL

from .__about__ import __version__ as version, __description__ as description

try:
    from .salesforce_handler import SalesforceHandler as Handler
    from .connection_args import connection_args_example, connection_args

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Salesforce"
name = "salesforce"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"
support_level = HANDLER_SUPPORT_LEVEL.MINDSDB

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "support_level",
    "description",
    "import_error",
    "icon_path",
    "connection_args",
    "connection_args_example",
]
