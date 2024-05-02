from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .together_ai_handler import TogetherAIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Together AI'
name = 'together_ai'
type = HANDLER_TYPE.ML
permanent = True
execution_method = 'subprocess_keep'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
