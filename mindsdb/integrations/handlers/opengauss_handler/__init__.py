from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .opengauss_handler import (
        OpenGaussHandler as Handler,
        connection_args_example,
        connection_args
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'openGauss'
name = 'opengauss'
type = HANDLER_TYPE.DATA

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'connection_args', 'connection_args_example', 'import_error', 'icon_path'
]
