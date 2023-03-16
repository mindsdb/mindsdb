from mindsdb.integrations.libs.const import HANDLER_TYPE

title = 'Altibase'
name = 'altibase'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'
description = 'Integration for connection to Altibase'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example', 'icon_path'
]
