from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .gmail_agent_handler import (
        GmailAgentHandler as Handler
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Gmail Agent'
name = 'gmail_agent'
type = HANDLER_TYPE.DATA
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
