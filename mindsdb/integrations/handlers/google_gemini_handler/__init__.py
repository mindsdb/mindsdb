from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description
from mindsdb.utilities import log


logger = log.getLogger(__name__)

try:
    from .google_gemini_handler import GoogleGeminiHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Google Gemini"
name = "google_gemini"
type = HANDLER_TYPE.ML
icon_path = "icon.svg"
permanent = False
__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
]
