from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description
from .access_handler import AccessHandler as Handler
from .ibm_netezza_handler import IBMNetezzaHandler  # Add the import for IBMNetezzaHandler here
import_error = None

title = 'Data Sources'
name = 'data_sources'
type = HANDLER_TYPE.DATA
icon_path = 'IBM_netezza_icon.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path'
]
