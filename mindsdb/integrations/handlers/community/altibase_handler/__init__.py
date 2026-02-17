from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example
try:
    from .altibase_handler import AltibaseHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Altibase'
name = 'altibase'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'icon_path', 'title',
    'description', 'connection_args', 'connection_args_example', 'import_error'
]
