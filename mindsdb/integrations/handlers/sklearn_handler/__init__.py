from mindsdb.integrations.libs.const import HANDLER_TYPE

version = "1.0"
description = "Simple scikit-learn based handler for classification and regression"

try:
    from .sklearn_handler import SklearnHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Sklearn"
name = "sklearn"
type = HANDLER_TYPE.ML
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
