from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)


try:
    from .google_gemini_handler import GoogleGeminiHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "google_gemini"
name = "google_gemini"
type = HANDLER_TYPE.ML
permanent = True
__all__ = ["Handler", "name", "type", "title", "import_error"]
