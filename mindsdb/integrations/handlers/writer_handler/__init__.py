from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .writer_handler import WriterHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Writer"
name = "writer"
type = HANDLER_TYPE.ML
permanent = False
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
