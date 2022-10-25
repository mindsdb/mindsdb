from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .auto_sklearn_handler import AutoSklearnHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Auto-Sklearn'
name = 'auto_sklearn'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
