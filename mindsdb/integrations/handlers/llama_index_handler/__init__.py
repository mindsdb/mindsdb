from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)

from .__about__ import __version__ as version, __description__ as description
try:
    from .llama_index_handler import  LlamaIndexHandler as Handler
    import_error = None
    logger.info("LlamaIndex  ML engine successfully imported")
except Exception as e:
    Handler = None
    import_error = e
    logger.info("Error Importing LlamaIndex ML engine")

title = 'LlamaIndex'
name = 'llama_index'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
