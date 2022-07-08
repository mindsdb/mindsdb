from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .questdb_handler import QuestDBHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e


title = 'QuestDB'
name = 'questdb'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'import_error', 'icon_path'
]
