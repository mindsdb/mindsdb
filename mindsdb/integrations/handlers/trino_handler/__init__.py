from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .trino_handler import TrinoHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'Trino'
name = 'trino'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'icon_path'
]
