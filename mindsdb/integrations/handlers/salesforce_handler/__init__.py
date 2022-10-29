from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .salesforce_handler (
        import salesforce_handler as Handler)
except ImportError:
    from .salesforce_handler import SalesforceHandler as Handler
    
title = 'Salesforce'
name = 'salesforce'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'import_error', 'icon_path'
]
