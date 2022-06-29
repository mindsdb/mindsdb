from .mongodb_handler import MongoDBHandler as Handler
from .__about__ import __version__ as version

title = 'MongoDB'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
