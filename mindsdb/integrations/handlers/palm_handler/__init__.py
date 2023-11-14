from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .palm_handler import PalmHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'PaLM'
name = 'palm'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]