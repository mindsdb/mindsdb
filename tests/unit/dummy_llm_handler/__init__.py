from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description

try:
    from .dummy_llm_handler import DummyHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = ''
name = 'dummy_llm'
type = HANDLER_TYPE.ML
permanent = True

__all__ = ['Handler', 'version', 'name', 'type', 'title', 'description', 'import_error']
