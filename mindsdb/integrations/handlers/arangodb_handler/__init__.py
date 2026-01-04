from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.integrations.handlers.arangodb_handler.arangodb_handler import ArangoDBHandler

title = 'ArangoDB'
name = 'arangodb'
handler_type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'ArangoDBHandler', 'title', 'name', 'handler_type', 'icon_path'
]
