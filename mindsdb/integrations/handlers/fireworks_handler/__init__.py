from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .fireworks_handler import FireworksHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "fireworks"
name = "fireworks"
type = HANDLER_TYPE.ML
icon_path = 'icon.png'
permanent = True
__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
