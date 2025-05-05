from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .linkup_handler import LinkupHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title="Linkup"
name="linkup"
type=HANDLER_TYPE.ML
icon_path='icon.jpeg'
permanent=False
__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
