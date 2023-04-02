from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .edgelessdb_handler import (
        EdgelessDBHandler as Handler,
        connection_args_example,
        connection_args
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'EdgelessDB'
name = 'edgelessdb'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'icon_path', 'title',
    'description', 'connection_args', 'connection_args_example', 'import_error'
]
