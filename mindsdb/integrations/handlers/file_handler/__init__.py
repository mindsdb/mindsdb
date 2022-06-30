from mindsdb.integrations.libs.const import HANDLER_TYPE

from .file_handler import FileHandler as Handler
from .__about__ import __version__ as version


title = 'File'
name = 'files'
type = HANDLER_TYPE.DATA

__all__ = [
    'Handler', 'version', 'name', 'type', 'title'
]
