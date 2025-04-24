import pytest
from filelock import FileLock

lock = FileLock("train_finetune.lock")


@pytest.fixture(scope="session")
def train_finetune_lock():
    """
    Fixture to lock the training and fine-tuning process for the session.
    Because mindsdb can't have multiple models training/fine-tuning at the same time,
    """
    return lock
