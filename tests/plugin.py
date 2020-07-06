import pytest


def pytest_configure(config):
    if config.getoption("randomly_seed") == 'default':
        config.option.randomly_seed = 420
