from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .scylla_handler import (
        ScyllaHandler as Handler,
        connection_args
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'ScyllaDB'
name = 'scylladb'
type = HANDLER_TYPE.DATA
icon_path = 'logo.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'connection_args', 'import_error', 'icon_path'
]
