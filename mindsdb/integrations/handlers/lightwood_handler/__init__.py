from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .lightwood_handler.lightwood_handler import LightwoodHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .lightwood_handler.__about__ import __version__ as version


title = 'Lightwood'
name = 'lightwood'
type = HANDLER_TYPE.ML
permanent = True


__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'import_error'
]
