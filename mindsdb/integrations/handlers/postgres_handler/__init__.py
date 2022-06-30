from .postgres_handler import PostgresHandler as Handler
from .__about__ import __version__ as version

title = 'PostgreSQL'
type = Handler.type
name = Handler.name

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
]
