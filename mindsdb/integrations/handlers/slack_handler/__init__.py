from mindsdb.integrations.libs.const import HANDLER_TYPE, HANDLER_MAINTAINER

from .__about__ import __version__ as version, __description__ as description

try:
    from .slack_handler import (
        SlackHandler as Handler
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Slack'
name = 'slack'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'
maintainer = HANDLER_MAINTAINER.MINDSDB

__all__ = [
    'Handler',
    'version',
    'name',
    'type',
    'maintainer',
    'title',
    'description',
    'import_error',
    'icon_path',
]
