from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .lindorm_handler import LindormDBHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Lindorm"
name = "lindorm"
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path"
]
