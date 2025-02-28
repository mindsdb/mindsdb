from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description

try:
    # from .jira_handler import JiraHandler as Handler
    # import_error = None
    from mindsdb.integrations.libs.api_handler_generator import APIHandlerGenerator, APIResourceGenerator, OpenAPISpecParser

    openapi_spec_parser = OpenAPISpecParser(
        "https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json"
    )
    api_handler_generator = APIHandlerGenerator(openapi_spec_parser)
    api_resource_generator = APIResourceGenerator(openapi_spec_parser)

    resources = api_resource_generator.generate_api_resources()
    Handler = api_handler_generator.generate_api_handler(resources)

    import_error = None
except Exception as e:
    Handler = None
    import_error = e


title = 'Atlassian Jira'
name = 'jira'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
]
