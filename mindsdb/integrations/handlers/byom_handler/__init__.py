from mindsdb.integrations.libs.const import HANDLER_TYPE

from .byom_handler import BYOMHandler as Handler, connection_args
from .__about__ import __version__ as version, __description__ as description

title = 'BYOM'
name = 'byom'
type = HANDLER_TYPE.ML

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'connection_args'
]

