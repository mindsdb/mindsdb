from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.integrations.handlers.mlflow_handler.__about__ import __version__ as version, __description__ as description
try:
    from mindsdb.integrations.handlers.mlflow_handler.mlflow_handler import MLflowHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'MLFlow'
name = 'mlflow'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
