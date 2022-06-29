from .lightwood_handler.lightwood_handler import LightwoodHandler as Handler
from .lightwood_handler.__about__ import __version__ as version


title = 'Lightwood'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
