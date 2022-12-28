from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description
from mindsdb.utilities.log import get_log
log = get_log()
try:
    from .jira_handler import (
        JiraHandler as Handler,
        connection_args_example,
        connection_args
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e


title = 'Atlassian Jira'
name = 'jira'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'


__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'connection_args', 'connection_args_example', 'import_error', 'icon_path'
]
