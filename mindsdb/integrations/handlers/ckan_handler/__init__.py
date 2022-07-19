from mindsdb.integrations.libs.const import HANDLER_TYPE

from mindsdb.integrations.handlers.ckan_handler.__about__ import __version__ as version, __description__ as description
try:
    from mindsdb.integrations.handlers.ckan_handler import CkanHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'CKAN'
name = 'ckan'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'import_error', 'icon_path'
]
