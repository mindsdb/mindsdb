from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .litellm_handler import LiteLLMHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "LiteLLM"
name = "litellm"
type = HANDLER_TYPE.ML
icon_path = "icon.png"
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
