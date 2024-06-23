from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args
try:
    from .gmail_handler import GmailHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Gmail'
name = 'gmail'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'connection_args',
    'import_error', 'icon_path'
]
