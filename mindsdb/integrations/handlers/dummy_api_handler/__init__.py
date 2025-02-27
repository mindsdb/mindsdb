from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.integrations.libs.api_handler_generator import APIHandlerGenerator, APIResourceGenerator

from .__about__ import __version__ as version, __description__ as description

try:
    api_resource_generator = APIResourceGenerator('openapi.json')
    resources = {
        'any': api_resource_generator.generate_api_resource()
    }
    api_handler_generator = APIHandlerGenerator('openapi.json')
    Handler = api_handler_generator.generate_api_handler(resources)

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Dummy API Handler'
name = 'dummy_api_handler'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'
permanent = False

__all__ = ['Handler', 'version', 'name', 'type', 'title', 'description', 'import_error', 'icon_path']
