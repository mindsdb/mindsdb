from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities.log import get_log

logger = get_log()

from .__about__ import __version__ as version, __description__ as description
try:
    from .anthropic_handler import AnthropicHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Anthropic'
name = 'anthropic'
type = HANDLER_TYPE.ML
permanent = True
__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
