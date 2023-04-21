from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .fbprophet_handler import FBProphetHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "FBProphet"
name = "fbprophet"
type = HANDLER_TYPE.ML
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error"]
