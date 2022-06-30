from .view_handler import ViewHandler as Handler
from .__about__ import __version__ as version


title = 'Views'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
