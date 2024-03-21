from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .web_handler import (
        WebHandler as Handler
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e


title = 'web'
name = 'web'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path'
]
