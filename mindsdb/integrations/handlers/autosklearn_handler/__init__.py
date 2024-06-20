from mindsdb.integrations.libs.const import HANDLER_TYPE

from mindsdb.integrations.handlers.autosklearn_handler.__about__ import __version__ as version, __description__ as description
try:
    from .autosklearn_handler import AutoSklearnHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Auto-Sklearn'
name = 'autosklearn'
type = HANDLER_TYPE.ML
icon_path = 'icon.png'
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error', 'icon_path'
]
