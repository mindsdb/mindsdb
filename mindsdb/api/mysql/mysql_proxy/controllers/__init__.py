import os

from .session_controller import (
        SessionController,
        ServerSessionContorller,
        )
if os.environ.get("MINDSDB_EXECUTOR_SERVICE"):
    SessionController = ServerSessionContorller
