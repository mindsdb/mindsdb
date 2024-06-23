from mindsdb.integrations.libs.const import HANDLER_TYPE

from mindsdb.integrations.handlers.tpot_handler.__about__ import __version__ as version, __description__ as description
try:
    from .tpot_handler import TPOTHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'TPOT'
name = 'TPOT'
type = HANDLER_TYPE.ML
icon_path = 'icon.png'
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error', 'icon_path'
]
