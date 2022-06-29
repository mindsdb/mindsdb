from .mariadb_handler import MariaDBHandler as Handler
from .__about__ import __version__ as version


title = 'MariaDB'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
