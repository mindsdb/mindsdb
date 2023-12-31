from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .langchain_embedding_handler import LangchainEmbeddingHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "LangChain Embedding"
name = "langchain_embedding"
type = HANDLER_TYPE.ML
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error"]
