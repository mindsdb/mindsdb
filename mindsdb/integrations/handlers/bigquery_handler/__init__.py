from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .bigquery_handler import BigQueryHandler as Handler, connection_args, connection_args_example
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'BigQuery'
name = 'bigquery'
type = HANDLER_TYPE.DATA
icon_path = 'logo.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path', 'connection_args', 'connection_args_example'
]
