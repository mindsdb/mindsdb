from .mysql_handler import MySQLHandler as Handler, connection_args_example
from .__about__ import __version__ as version, __description__ as description


title = 'MySQL'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
