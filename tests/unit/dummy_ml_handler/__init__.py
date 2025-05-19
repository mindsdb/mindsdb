from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .creation_args import creation_args
from .model_using_args import model_using_args
try:
    from .dummy_ml_handler import DummyHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = ''
name = 'dummy_ml'
type = HANDLER_TYPE.ML
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error',
    'creation_args', 'model_using_args'
]
