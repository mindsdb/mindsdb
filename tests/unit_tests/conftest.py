import logging
import pytest

from mindsdb.libs.controllers.transaction import Transaction
from mindsdb.libs.data_types.mindsdb_logger import MindsdbLogger
from mindsdb.libs.controllers.predictor import Predictor
from mindsdb.config import CONFIG


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
