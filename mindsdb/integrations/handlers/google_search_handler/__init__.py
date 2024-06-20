from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example
try:
    from .google_search_handler import GoogleSearchConsoleHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Google Search'
name = 'google_search'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path', 'connection_args_example', 'connection_args'
]
