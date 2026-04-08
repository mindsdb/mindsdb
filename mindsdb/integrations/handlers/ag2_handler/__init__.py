from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .creation_args import creation_args
from .model_using_args import model_using_args

try:
    from .ag2_handler import AG2Handler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "AG2"
name = "ag2"
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
    "creation_args",
    "model_using_args",
]
