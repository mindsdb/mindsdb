import pytest

def pytest_addoption(parser):
    """
    Adds command-line options to pytest.
    This hook is in the top-level conftest to ensure flags are always available.
    """
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--run-dsi-tests", action="store_true", default=False, help="run DSI integration tests")

def pytest_collection_modifyitems(config, items):
    """
    Modifies test items after collection to skip tests based on flags.
    """
    # Handle 'slow' tests.
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Handle 'dsi' tests.
    if not config.getoption("--run-dsi-tests"):
        items[:] = [item for item in items if "dsi" not in item.keywords]