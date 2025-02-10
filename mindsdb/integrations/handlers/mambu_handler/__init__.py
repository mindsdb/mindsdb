from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description

try:
    from .mambu_handler import MambuHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Mambu"
name = "mambu"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"

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
