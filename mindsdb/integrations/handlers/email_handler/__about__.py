"""
Email handler package metadata and handler UI hints.
Note: Keep this file import-safe and free of heavy imports.
"""

from enum import Enum

__title__ = "MindsDB Email handler"
__package_name__ = "mindsdb_email_handler"
__version__ = "0.0.5"
__description__ = "MindsDB handler for email"
__author__ = "MindsDB Inc"
__github__ = "https://github.com/mindsdb/mindsdb"
__pypi__ = "https://pypi.org/project/mindsdb/"
__license__ = "MIT"
__icon_path__ = "icon.png"

# Robust, import-safe HANDLER_TYPE assignment:
# Prefer the enum-like source; if unavailable, fall back to a compatible enum member, not a bare string.
try:
    from mindsdb.integrations.libs.const import HANDLER_TYPE as _HANDLER_TYPE_ENUM  # type: ignore

    HANDLER_TYPE = getattr(_HANDLER_TYPE_ENUM, "DATA")
except Exception:
    # Fallback to a compatible enum member to avoid breaking code that expects Enum semantics.
    _FallbackHT = Enum("HANDLER_TYPE", {"DATA": "data"})
    HANDLER_TYPE = _FallbackHT.DATA
