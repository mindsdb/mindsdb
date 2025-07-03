from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .leonardo_ai_handler import LeonardoAIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Leonardo AI'
name = 'leonardo_ai'
type = HANDLER_TYPE.ML
icon_path = "icon.svg"
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error', 'icon_path'
]
