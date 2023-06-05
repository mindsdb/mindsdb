from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version
from .settings import PERSIST_DIRECTORY, check_path_exists

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
check_path_exists(PERSIST_DIRECTORY)


__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
]
