from mindsdb.integrations.libs.const import HANDLER_TYPE, HANDLER_SUPPORT_LEVEL

from .file_handler import FileHandler as Handler
from .__about__ import __version__ as version


title = "File"
name = "files"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"
support_level = HANDLER_SUPPORT_LEVEL.MINDSDB
permanent = True

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "icon_path",
    "support_level",
]
