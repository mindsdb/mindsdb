from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version

try:
    from .autogluon_handler import AutoGluonHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "AutoGluon"
name = "autogluon"
type = HANDLER_TYPE.ML
icon_path = 'icon.svg'
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
