from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)

from .__about__ import __version__ as version, __description__ as description
try:
    from .cohere_handler import CohereHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Cohere'
name = 'cohere'
type = HANDLER_TYPE.ML

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
