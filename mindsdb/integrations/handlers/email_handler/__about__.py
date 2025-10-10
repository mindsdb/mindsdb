"""
Email handler package metadata and handler UI hints.
Note: Keep this file import-safe and free of heavy imports.
"""

__title__ = "MindsDB Email handler"
__package_name__ = "mindsdb_email_handler"
__version__ = "0.0.2"
__description__ = "MindsDB handler for email"
__author__ = "MindsDB Inc"
__github__ = "https://github.com/mindsdb/mindsdb"
__pypi__ = "https://pypi.org/project/mindsdb/"
__license__ = "MIT"
__icon_path__ = "icon.png"  # fixed from icon.svg to match repository asset

# Robust, import-safe HANDLER_TYPE assignment:
# Try to import the enum-like source; if unavailable, fall back to "data".
try:
    # Some handlers use a constant/enum from libs.const. Keep this resilient.
    from mindsdb.integrations.libs.const import HANDLER_TYPE as _HANDLER_TYPE_ENUM  # type: ignore

    HANDLER_TYPE = getattr(_HANDLER_TYPE_ENUM, "DATA", "data")
except Exception:
    HANDLER_TYPE = "data"
