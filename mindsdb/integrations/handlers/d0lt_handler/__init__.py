from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description

try:
    from .d0lt_handler import (
        D0ltHandler as Handler,
    )

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "D0lt"
name = "d0lt"
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
