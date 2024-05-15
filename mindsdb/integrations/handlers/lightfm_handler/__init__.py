from .__about__ import (
    __description__ as description,
    __version__ as version,
)
from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .lightfm_handler import LightFMHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "LightFM-Recommender"
name = "lightfm"
type = HANDLER_TYPE.ML
icon_path = "icon.svg"
permanent = False

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
