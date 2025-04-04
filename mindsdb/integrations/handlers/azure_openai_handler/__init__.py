from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description
from .creation_args import creation_args
try:
    from .azure_openai_handler import AzureOpenAIHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'import_error', 'icon_path', 'creation_args', 'model_using_args'
]

title = "Azure OpenAI"
name = "azure_openai"
type = HANDLER_TYPE.ML
icon_path = "icon.svg"