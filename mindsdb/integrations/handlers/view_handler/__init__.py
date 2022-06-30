from mindsdb.integrations.libs.const import HANDLER_TYPE

from .view_handler import ViewHandler as Handler
from .__about__ import __version__ as version, __description__ as description


title = 'View'
name = 'views'
type = HANDLER_TYPE.DATA

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
