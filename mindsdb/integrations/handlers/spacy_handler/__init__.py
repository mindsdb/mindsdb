from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
try:
    from .spacy_handler import SpacyHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'spaCy'
name = 'spacy'
type = HANDLER_TYPE.ML
icon_path = 'icon.svg'
permanent = False

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error', 'icon_path'
]
