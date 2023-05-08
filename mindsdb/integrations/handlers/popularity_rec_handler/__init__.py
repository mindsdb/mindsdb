from mindsdb.integrations.libs.const import HANDLER_TYPE

from mindsdb.integrations.handlers.autosklearn_handler.__about__ import __version__ as version, __description__ as description
try:
    from .popularity_rec_handler import LightFMHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Popularity-Recommender'
name = 'popularity-recommender'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]
