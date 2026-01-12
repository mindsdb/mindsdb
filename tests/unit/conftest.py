from __future__ import annotations

from pathlib import Path

import pytest


_EXECUTOR_ROOT = Path("tests/unit/executor").resolve()
_EXECUTOR_GROUP = "executor"


def _is_executor_item(item: pytest.Item) -> bool:
    try:
        p = Path(str(getattr(item, "fspath", item.location[0]))).resolve()
    except Exception:
        p = Path(item.location[0]).resolve()
    return _EXECUTOR_ROOT in p.parents or p == _EXECUTOR_ROOT


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Isolate executor tests by grouping them into their own xdist worker and ordering first."""
    executor_items: list[pytest.Item] = []
    other_items: list[pytest.Item] = []

    for item in items:
        if _is_executor_item(item):
            item.add_marker(pytest.mark.executor)
            item.add_marker(pytest.mark.xdist_group(_EXECUTOR_GROUP))
            executor_items.append(item)
        else:
            other_items.append(item)

    # Run executor tests first; grouping keeps them on a dedicated worker.
    items[:] = executor_items + other_items
