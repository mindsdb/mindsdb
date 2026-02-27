from mindsdb.integrations.libs.const import HANDLER_SUPPORT_LEVEL, HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example
try:
    from .duckdb_handler import DuckDBHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'DuckDB'
name = 'duckdb'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'
support_level = HANDLER_SUPPORT_LEVEL.MINDSDB

__all__ = [
    'Handler',
    'version',
    'name',
    'type',
    'title',
    'description',
    'connection_args',
    'connection_args_example',
    'import_error',
    'support_level',
    'icon_path',
]
