from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .apache_doris_handler import ApacheDorisHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'Apache Doris'
name = 'apache_doris'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path'
]
