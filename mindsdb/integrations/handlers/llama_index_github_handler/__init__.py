from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .llama_index_github_handler import LlamaIndexGithubHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'LlamaIndexGithubLoader'
name = 'llama_index_github'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
