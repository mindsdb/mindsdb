import pytest


def pytest_configure(config):
    if config.getoption("randomly_seed") == 'default':
        config.option.randomly_seed = 42

    if not config.getoption('randomly_reorganize'):
        config.option.randomly_reorganize = False
