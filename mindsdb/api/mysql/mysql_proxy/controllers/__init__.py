import os

from .session_controller import (
        SessionController,
        ServerSessionContorller,
        )
if os.environ.get("MINDSDB_EXECUTOR_SERVICE_HOST") and os.environ.get("MINDSDB_EXECUTOR_SERVICE_PORT"):
    SessionController = ServerSessionContorller
