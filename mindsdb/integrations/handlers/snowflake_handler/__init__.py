from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .snowflake_handler import SnowflakeHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'Snowflake'
name = 'snowflake'
type = HANDLER_TYPE.DATA

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'icon_path'
]
