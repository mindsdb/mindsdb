from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
# try:
#     from .huggingface_handler import HuggingFaceHandler as Handler
#     import_error = None
# except Exception as e:
#     Handler = None
#     import_error = e

# NOTE: security vulnerability is in `pytorch` v2.7.1, revert changes here and in
# requirements.txt/requirements_cpu.txt when new version is released
Handler = None
import_error = """
    The `huggingface_handler` is temporary disabled in current version of MindsDB due to security vulnerability.
"""

title = "Hugging Face"
name = "huggingface"
type = HANDLER_TYPE.ML
icon_path = "icon.svg"
permanent = False
execution_method = "subprocess_keep"

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error", "icon_path"]
