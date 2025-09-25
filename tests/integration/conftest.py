import pytest
import os
from filelock import FileLock

int_url_split = os.environ.get("INTERNAL_URL", "localhost").split(":")

INTERNAL_URL = int_url_split[0]
if len(int_url_split) == 1:
    HTTP_PORT = "80" if "svc.cluster.local" in INTERNAL_URL else "47334"
else:
    HTTP_PORT = int_url_split[1]

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
