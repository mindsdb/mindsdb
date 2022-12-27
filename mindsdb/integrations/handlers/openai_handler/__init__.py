from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .openai_handler import OpenAIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'OpenAI'
name = 'openai'
type = HANDLER_TYPE.ML
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
