"""
conftest for tests/unit/various/

Injects a lightweight stub for mindsdb.integrations.utilities.handler_utils
before test collection so that test_llm_client.py can import LLMClient without
pulling in the full MindsDB storage/DB stack (which requires the private
mind_castle package).

setdefault is used intentionally: if the real module is already present in
sys.modules (e.g. when the full stack is installed), the stub is NOT injected
and the real module is used instead.
"""

import sys
import types
from unittest.mock import MagicMock

_HANDLER_UTILS_KEY = "mindsdb.integrations.utilities.handler_utils"

_stub = types.ModuleType(_HANDLER_UTILS_KEY)
_stub.get_api_key = MagicMock(return_value=None)

sys.modules.setdefault(_HANDLER_UTILS_KEY, _stub)
