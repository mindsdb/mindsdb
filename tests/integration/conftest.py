import pytest
import os
from filelock import FileLock

INTERNAL_URL = os.environ.get("INTERNAL_URL", "localhost")
HTTP_PORT = "80" if "svc.cluster.local" in INTERNAL_URL else "47334"

HTTP_API_ROOT = f"http://{INTERNAL_URL}:{HTTP_PORT}/api"
MYSQL_API_ROOT = INTERNAL_URL

lock = FileLock("train_finetune.lock")


@pytest.fixture(scope="session")
def train_finetune_lock():
    """
    Fixture to lock the training and fine-tuning process for the session.
    Because mindsdb can't have multiple models training/fine-tuning at the same time,
    """
    return lock
