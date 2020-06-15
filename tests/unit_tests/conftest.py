import logging
import pytest

from mindsdb.libs.controllers.transaction import Transaction
from mindsdb.libs.data_types.mindsdb_logger import MindsdbLogger
from mindsdb.libs.controllers.predictor import Predictor
from mindsdb.config import CONFIG


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(autouse=True)
def config(monkeypatch):
    CONFIG.CHECK_FOR_UPDATES = False


@pytest.fixture()
def logger():
    return MindsdbLogger(log_level=logging.DEBUG, uuid='test')


@pytest.fixture()
def session():
    return Predictor(name='test')


@pytest.fixture()
def transaction(logger):
    lmd, hmd = {}, {}

    lmd['type'] = 'foobar'
    transaction = Transaction(session=session,
                              light_transaction_metadata=lmd,
                              heavy_transaction_metadata=hmd,
                              logger=logger)
    return transaction