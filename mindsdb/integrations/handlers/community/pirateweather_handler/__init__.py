from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version
from .connection_args import connection_args, connection_args_example
try:
    from .pirateweather_handler import PirateWeatherAPIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Pirate Weather"
name = "pirateweather"
type = HANDLER_TYPE.DATA
icon_path = "icon.png"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
    "connection_args_example",
    "connection_args",
]
