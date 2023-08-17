from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)

from .__about__ import __version__ as version, __description__ as description

try:
    from .influxdb_handler import InfluxDBHandler as Handler
    logger.info("No error Importing InfluxDB Handler")
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
    logger.info(f"Error importing InfluxDB Handler: {e}!")

title = 'InfluxDB'
name = 'influxdb'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'

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
