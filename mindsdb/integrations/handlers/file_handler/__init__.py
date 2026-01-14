from mindsdb.integrations.libs.const import HANDLER_TYPE, HANDLER_MAINTAINER

from .file_handler import FileHandler as Handler
from .__about__ import __version__ as version


title = "File"
name = "files"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"
maintainer = HANDLER_MAINTAINER.MINDSDB
permanent = True

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "icon_path",
    "maintainer",
]
