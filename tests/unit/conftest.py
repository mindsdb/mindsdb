from __future__ import annotations

import os
import platform
from pathlib import Path

import pytest


_EXECUTOR_ROOT = Path("tests/unit/executor")
_EXECUTOR_GROUP = "executor"


def _is_executor_item(item: pytest.Item) -> bool:
    try:
        Path(item.location[0]).relative_to(_EXECUTOR_ROOT)
    except ValueError:
        return False
    return True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Isolate executor tests so they do not leak state into other modules."""
    executor_items: list[pytest.Item] = []
    other_items: list[pytest.Item] = []

    for item in items:
        if _is_executor_item(item):
            item.add_marker("executor")
            # Run executor tests in a dedicated xdist group to isolate them on all platforms.
            item.add_marker(pytest.mark.xdist_group(_EXECUTOR_GROUP))
            executor_items.append(item)
        else:
            other_items.append(item)

    # Run executor tests first while keeping them forked.
    items[:] = executor_items + other_items
