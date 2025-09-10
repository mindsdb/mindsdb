# In tests/conftest.py

import pytest

def pytest_addoption(parser):
    """
    Adds command-line options to pytest.
    """
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--run-dsi-tests", action="store_true", default=False, help="run DSI integration tests")

def pytest_collection_modifyitems(config, items):
    """
    Modifies test items after collection by adding skip markers. This is the only
    safe way to conditionally run tests with pytest-xdist.
    """
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-dsi-tests"):
        skip_dsi = pytest.mark.skip(reason="need --run-dsi-tests option to run")
        for item in items:
            if "dsi" in item.keywords:
                item.add_marker(skip_dsi)