from .__about__ import __version__ as version
from .__about__ import __description__ as description
from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)


try:
    from .minds_endpoint_handler import MindsEndpointHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Minds Endpoint"
name = "minds_endpoint"
type = HANDLER_TYPE.ML
icon_path = 'icon.svg'
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
