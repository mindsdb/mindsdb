from .__about__ import __description__ as description
from .__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_TYPE
from .creation_args import creation_args
from .model_using_args import model_using_args

try:
    from .togetherai_handler import TogetherAIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "TogetherAI"
name = "togetherai"
type = HANDLER_TYPE.ML
icon_path = "icon.svg"
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path", "creation_args", "model_using_args"]
