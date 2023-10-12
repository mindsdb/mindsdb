from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .sentence_transformer_handler import SentenceTransformerHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Sentence Transformer"
name = "sentencetransformer"
type = HANDLER_TYPE.ML
permanent = True
execution_method = "subprocess_keep"

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error"]
